use crossbeam_channel::{Sender, Receiver, RecvTimeoutError};
use crate::EndOfStreamError;
use crate::buf_batch::{PollResult, BufBatch};

use std::fmt::Debug;
use std::time::{Duration, Instant};

/// Commands that can be send to `BufBatchChannel` via `Sender` endpoint.
#[derive(Debug)]
pub enum Command<I: Debug> {
    /// Append item `I` to batch.
    Append(I),
    /// Flush outstanding items.
    Complete,
}

/// Result of batching operation
#[derive(Debug)]
pub enum BatchResult<'i, I> {
    /// New item appended to batch
    Item(&'i I),
    /// Batch is now complete
    Complete,
}

#[derive(Debug)]
pub struct BufBatchChannel<I: Debug> {
    channel: Receiver<Command<I>>,
    batch: BufBatch<I>,
    // True when channel is disconnected
    disconnected: bool,
    // True if batch is complete but commit or retry not called yet
    complete: bool,
    // Instant at which a batch would reach its duration limit
    ready_at: Option<Instant>,
}

impl<I: Debug> BufBatchChannel<I> {
    pub fn new(max_size: usize, max_duration: Duration) -> (Sender<Command<I>>, BufBatchChannel<I>) {
        let (sender, receiver) = crossbeam_channel::bounded(max_size * 2);

        (sender, BufBatchChannel {
            channel: receiver,
            batch: BufBatch::new(max_size, max_duration),
            disconnected: false,
            complete: false,
            ready_at: None,
        })
    }

    pub fn with_producer_thread(max_size: usize, max_duration: Duration, producer: impl Fn(Sender<Command<I>>) -> () + Send + 'static) -> BufBatchChannel<I> where I: Send + 'static {
        let (sender, batch) = BufBatchChannel::new(max_size, max_duration);

        std::thread::spawn(move || {
            producer(sender)
        });

        batch
    }

    /// Get next item from the batch.
    ///
    /// Returns `Ok(BatchResult::Item(I))` with next item of the batch.
    ///
    /// Returns `Ok(BatchResult::Complete)` signaling end of batch if:
    /// * `max_size` of the batch was reached,
    /// * `max_duration` since first element returned elapsed.
    /// Caller is responsible for calling `retry` or `commit` after receiving `BatchResult::Complete`
    ///
    /// This call will block indefinitely waiting for first item of the batch.
    ///
    /// After calling `retry` this will provide batch items again from the first one. It will
    /// continue fetching items from producer until max_size or max_duration is reached starting
    /// with original first item time.
    ///
    /// After calling `clear` this function will behave as if new `Batch` object was started.
    pub fn next(&mut self) -> Result<BatchResult<I>, EndOfStreamError> {
        // No iternal messages left to yeld and channel is disconnected
        if self.disconnected {
            return Err(EndOfStreamError)
        }

        loop {
            // Check if we have a ready batch due to any limit or go fetch next item
            let ready_at = match self.batch.poll() {
                PollResult::Ready => return Ok(BatchResult::Complete),
                PollResult::NotReady(instant) => instant,
            };

            let recv_result = if let Some(instant) = ready_at {
                let now = Instant::now();
                if now >= instant {
                    // Race between Instant::now() and .poll()
                    continue
                }
                match self.channel.recv_timeout(instant.duration_since(now)) {
                    // We got new item before timeout was reached
                    Ok(item) => Ok(item),
                    // A batch should be ready now; go again
                    Err(RecvTimeoutError::Timeout) => continue,
                    // Other end gone
                    Err(RecvTimeoutError::Disconnected) => Err(EndOfStreamError),
                }
            } else {
                // No outstanding batches; wait for first item
                self.channel.recv().map_err(|_| EndOfStreamError)
            };

            match recv_result {
                Ok(Command::Append(item)) => match self.batch.append(item) {
                    Ok(item) => return Ok(BatchResult::Item(item)),
                    Err(_item) => panic!("poll returned NotReady but batch is not accepting items"),
                },
                Ok(Command::Complete) => {
                    // Mark as complete by producer
                    return Ok(BatchResult::Complete)
                },
                Err(_eos) => {
                    self.disconnected = true;

                    // There won't be next batch
                    self.ready_at.take();

                    return Ok(BatchResult::Complete)
                }
            };
        }
    }

    /// Start new batch discarding buffered items.
    /// 
    /// Returns `Err(EndOfStreamError)` if channel was closed and there won't be any more items.
    pub fn clear(&mut self) {
        self.batch.clear();
        self.complete = false;
        self.ready_at = None;
    }
}

use crossbeam_channel::{Sender, Receiver, RecvTimeoutError};
use crate::EndOfStreamError;
use crate::buf_batch::{PollResult, BufBatch};

use std::fmt::Debug;
use std::time::Duration;
use std::vec::Drain;

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
}

impl<I: Debug> BufBatchChannel<I> {
    pub fn new(max_size: usize, max_duration: Duration) -> (Sender<Command<I>>, BufBatchChannel<I>) {
        let (sender, receiver) = crossbeam_channel::bounded(max_size * 2);

        (sender, BufBatchChannel {
            channel: receiver,
            batch: BufBatch::new(max_size, max_duration),
            disconnected: false,
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
    /// * `max_duration` since first element returned elapsed,
    /// * client sent `Command::Complete`.
    /// 
    /// Caller is responsible for consuming the batch via provided methods or calling `clear()`.
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
            let ready_after = match self.batch.poll() {
                PollResult::Ready => return Ok(BatchResult::Complete),
                PollResult::NotReady(ready_after) => ready_after,
            };

            let recv_result = if let Some(ready_after) = ready_after {
                match self.channel.recv_timeout(ready_after) {
                    // We got new item before timeout was reached
                    Ok(item) => Ok(item),
                    // A batch should be ready now; try again
                    Err(RecvTimeoutError::Timeout) => continue,
                    // Other end gone
                    Err(RecvTimeoutError::Disconnected) => Err(EndOfStreamError),
                }
            } else {
                // No outstanding batches; wait for first item
                self.channel.recv().map_err(|_| EndOfStreamError)
            };

            match recv_result {
                Ok(Command::Append(item)) => {
                    let item = self.batch.append(item);
                    return Ok(BatchResult::Item(item))
                }
                Ok(Command::Complete) => {
                    // Mark as complete by producer
                    return Ok(BatchResult::Complete)
                },
                Err(_eos) => {
                    self.disconnected = true;
                    return Ok(BatchResult::Complete)
                }
            };
        }
    }

    /// Start new batch discarding buffered items.
    pub fn clear(&mut self) {
        self.batch.clear();
    }

    /// Return items as new `Vec` and start new batch
    pub fn split_off(&mut self) -> Vec<I> {
        self.batch.split_off()
    }

    /// Drain items from internal buffer and start new batch
    /// Assuming that `Drain` iterator is not leaked leading to stale items left in items buffer.
    pub fn drain(&mut self) -> Drain<I> {
        self.batch.drain()
    }

    /// Swap items buffer with given `Vec` and clear
    pub fn swap(&mut self, items: &mut Vec<I>) {
        self.batch.swap(items)
    }

    /// Convert into intrnal item buffer
    pub fn into_vec(self) -> Vec<I> {
        self.batch.into_vec()
    }

    /// Return slice from intranl item buffer
    pub fn as_slice(&self) -> &[I] {
        self.batch.as_slice()
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    use std::time::Duration;
    use assert_matches::assert_matches;

    #[test]
    fn test_batch_clear() {
        let (sender, mut batch) = BufBatchChannel::new(2, Duration::from_secs(10));

        sender.send(Command::Append(1)).unwrap();
        sender.send(Command::Append(2)).unwrap();
        sender.send(Command::Append(3)).unwrap();
        sender.send(Command::Append(4)).unwrap();

        assert_matches!(batch.next(), Ok(BatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(BatchResult::Item(2)));
        assert_matches!(batch.next(), Ok(BatchResult::Complete)); // max_size

        batch.clear();

        assert_matches!(batch.next(), Ok(BatchResult::Item(3)));
        assert_matches!(batch.next(), Ok(BatchResult::Item(4)));
        assert_matches!(batch.next(), Ok(BatchResult::Complete)); // max_size
    }

    #[test]
    fn test_batch_with_producer_thread() {
        let mut batch = BufBatchChannel::with_producer_thread(2, Duration::from_secs(10), |sender| {
            sender.send(Command::Append(1)).unwrap();
            sender.send(Command::Append(2)).unwrap();
            sender.send(Command::Append(3)).unwrap();
            sender.send(Command::Append(4)).unwrap();
        });

        assert_matches!(batch.next(), Ok(BatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(BatchResult::Item(2)));
        assert_matches!(batch.next(), Ok(BatchResult::Complete)); // max_size

        batch.clear();

        assert_matches!(batch.next(), Ok(BatchResult::Item(3)));
        assert_matches!(batch.next(), Ok(BatchResult::Item(4)));
        assert_matches!(batch.next(), Ok(BatchResult::Complete)); // max_size
    }

    #[test]
    fn test_batch_max_duration() {
        let mut batch = BufBatchChannel::with_producer_thread(2, Duration::from_millis(100), |sender| {
            sender.send(Command::Append(1)).unwrap();
            std::thread::sleep(Duration::from_millis(500));
        });

        assert_matches!(batch.next(), Ok(BatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(BatchResult::Complete)); // max_size
    }

    #[test]
    fn test_batch_disconnected() {
        let mut batch = BufBatchChannel::with_producer_thread(2, Duration::from_secs(10), |sender| {
            sender.send(Command::Append(1)).unwrap();
        });

        assert_matches!(batch.next(), Ok(BatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(BatchResult::Complete)); // disconnected
        assert_matches!(batch.next(), Err(EndOfStreamError));
    }

    #[test]
    fn test_batch_command_complete() {
        let mut batch = BufBatchChannel::with_producer_thread(2, Duration::from_secs(10), |sender| {
            sender.send(Command::Append(1)).unwrap();
            sender.send(Command::Complete).unwrap();
            sender.send(Command::Append(2)).unwrap();
            sender.send(Command::Complete).unwrap();
        });

        assert_matches!(batch.next(), Ok(BatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(BatchResult::Complete)); // Command::Complete

        batch.clear();

        assert_matches!(batch.next(), Ok(BatchResult::Item(2)));
        assert_matches!(batch.next(), Ok(BatchResult::Complete)); // Command::Complete
    }
}
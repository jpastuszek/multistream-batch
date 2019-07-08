//! `TxBatch` allows for batching incoming stream of items based on batch maximum size or maximum
//! duration since first item received. It also buffers the items so that current batch can be
//! processed again for example in case of downstream transaction failure.
use crossbeam_channel::{Sender, Receiver, RecvTimeoutError};
use crate::EndOfStreamError;
use crate::buf_batch::{PollResult, BufBatch};

use std::fmt::Debug;
use std::time::{Duration, Instant};

/// Commands that can be send to `TxBatch` via `Sender` endpoint.
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

// TODO: impl TxIterator that represents batch; when dropped batch is commited, has retry()
// function to start iteration from begginging of the batch

#[derive(Debug)]
pub struct TxBatch<I: Debug> {
    channel: Receiver<Command<I>>,
    batch: BufBatch<I>,
    // Retry uncommited number of messages before fetching next one/complete
    retry_uncommitted: Option<usize>,
    // True when channel is disconnected
    disconnected: bool,
    // True if batch is complete but commit or retry not called yet
    complete: bool,
    // Instant at which a batch would reach its duration limit
    ready_at: Option<Instant>,
}

impl<I: Debug> TxBatch<I> {
    pub fn new(max_size: usize, max_duration: Duration) -> (Sender<Command<I>>, TxBatch<I>) {
        let (sender, receiver) = crossbeam_channel::bounded(max_size * 2);

        (sender, TxBatch {
            channel: receiver,
            batch: BufBatch::new(max_size, max_duration),
            retry_uncommitted: None,
            disconnected: false,
            complete: false,
            ready_at: None,
        })
    }

    pub fn with_producer_thread(max_size: usize, max_duration: Duration, producer: impl Fn(Sender<Command<I>>) -> () + Send + 'static) -> TxBatch<I> where I: Send + 'static {
        let (sender, batch) = TxBatch::new(max_size, max_duration);

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
    /// After calling `commit` this function will behave as if new `Batch` object was crated.
    pub fn next(&mut self) -> BatchResult<I> {
        loop {
            // Yield internal messages if batch was retried
            if let Some(retry_uncommitted) = self.retry_uncommitted {
                let item = &self.batch.as_slice()[self.batch.as_slice().len() - retry_uncommitted];
                if retry_uncommitted == 1 {
                    self.retry_uncommitted = None;
                } else {
                    self.retry_uncommitted = Some(retry_uncommitted - 1);
                }
                return BatchResult::Item(item)
            }

            // No iternal messages left to yeld and channel is disconnected
            if self.complete || self.disconnected {
                return BatchResult::Complete
            }

            let now = Instant::now();

            let recv_result = match self.ready_at.map(|instant| if instant <= now { None } else { Some(instant) }) {
                // Batch ready to go
                Some(None) => {
                    // We should have ready batch but if not update ready_at and go again
                    match self.batch.poll() {
                        PollResult::Ready => {
                            self.complete = true;
                            return BatchResult::Complete
                        },
                        PollResult::NotReady(instant) => {
                            // Update instant here as batch could have been already returned by append
                            self.ready_at = instant;
                            continue
                        }
                    }
                }
                // Wait for batch duration limit or next item
                Some(Some(instant)) => match self.channel.recv_timeout(instant.duration_since(now)) {
                    // We got new item before timeout was reached
                    Ok(item) => Ok(item),
                    // A batch should be ready now; go again
                    Err(RecvTimeoutError::Timeout) => continue,
                    // Other end gone
                    Err(RecvTimeoutError::Disconnected) => Err(EndOfStreamError),
                },
                // No outstanding batches; wait for first item
                None => self.channel.recv().map_err(|_| EndOfStreamError),
            };

            match recv_result {
                Ok(Command::Append(item)) => match self.batch.append(item) {
                    // Batch is now full
                    (item, PollResult::Ready) => {
                        // Next call will return BatchResult::Complete
                        self.complete = true;
                        return BatchResult::Item(item)
                    },
                    // Batch not yet full; note instant at witch oldest batch will reach its duration limit
                    (item, PollResult::NotReady(instant)) => {
                        self.ready_at = instant;
                        return BatchResult::Item(item)
                    }
                },
                Ok(Command::Complete) => {
                    // Mark as complete by producer
                    self.complete = true;
                    return BatchResult::Complete
                },
                Err(_eos) => {
                    self.disconnected = true;

                    // There won't be next batch
                    self.ready_at.take();

                    self.complete = true;
                    return BatchResult::Complete
                }
            };
        }
    }

    /// Returns `true` if current batch is complete and needs to be committed or retried.
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    /// Start new batch.
    /// 
    /// Returns `Err(EndOfStreamError)` if channel was closed and there won't be any more items.
    pub fn commit(&mut self) -> Result<(), EndOfStreamError> {
        self.batch.clear();
        self.retry_uncommitted = None;
        self.complete = false;
        self.ready_at = None;

        if self.disconnected {
            return Err(EndOfStreamError)
        }
        Ok(())
    }

    /// Retry batch making `next` to iterate uncommited items starting from oldest one.
    pub fn retry(&mut self) {
        self.retry_uncommitted = Some(self.uncommitted());
    }

    /// Number of items in current batch.
    pub fn uncommitted(&self) -> usize {
        self.batch.as_slice().len()
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    use std::time::Duration;
    use assert_matches::assert_matches;

    #[test]
    fn test_batch_reset() {
        let (sender, mut batch) = TxBatch::new(4, Duration::from_secs(10));

        sender.send(Command::Append(1)).unwrap();
        sender.send(Command::Append(2)).unwrap();
        sender.send(Command::Append(3)).unwrap();
        sender.send(Command::Append(4)).unwrap();
        sender.send(Command::Append(5)).unwrap();

        assert_matches!(batch.next(), BatchResult::Item(1));
        assert_matches!(batch.next(), BatchResult::Item(2));
        assert_matches!(batch.next(), BatchResult::Item(3));

        batch.retry();

        assert_matches!(batch.next(), BatchResult::Item(1));
        assert_matches!(batch.next(), BatchResult::Item(2));
        assert_matches!(batch.next(), BatchResult::Item(3));

        batch.retry();

        assert_matches!(batch.next(), BatchResult::Item(1));

        batch.retry();

        assert_matches!(batch.next(), BatchResult::Item(1));
        assert_matches!(batch.next(), BatchResult::Item(2));
        assert_matches!(batch.next(), BatchResult::Item(3));
        assert_matches!(batch.next(), BatchResult::Item(4));
        assert_matches!(batch.next(), BatchResult::Complete); // max_size
    }

    #[test]
    fn test_batch_commit() {
        let (sender, mut batch) = TxBatch::new(2, Duration::from_secs(10));

        sender.send(Command::Append(1)).unwrap();
        sender.send(Command::Append(2)).unwrap();
        sender.send(Command::Append(3)).unwrap();
        sender.send(Command::Append(4)).unwrap();

        assert_matches!(batch.next(), BatchResult::Item(1));
        assert_matches!(batch.next(), BatchResult::Item(2));
        assert_matches!(batch.next(), BatchResult::Complete); // max_size

        batch.commit().unwrap();

        assert_matches!(batch.next(), BatchResult::Item(3));

        batch.retry();

        assert_matches!(batch.next(), BatchResult::Item(3));
        assert_matches!(batch.next(), BatchResult::Item(4));
        assert_matches!(batch.next(), BatchResult::Complete); // max_size
    }

    #[test]
    fn test_batch_with_producer_thread() {
        let mut batch = TxBatch::with_producer_thread(2, Duration::from_secs(10), |sender| {
            sender.send(Command::Append(1)).unwrap();
            sender.send(Command::Append(2)).unwrap();
            sender.send(Command::Append(3)).unwrap();
            sender.send(Command::Append(4)).unwrap();
        });

        assert_matches!(batch.next(), BatchResult::Item(1));
        assert_matches!(batch.next(), BatchResult::Item(2));
        assert_matches!(batch.next(), BatchResult::Complete); // max_size

        batch.commit().unwrap();

        assert_matches!(batch.next(), BatchResult::Item(3));

        batch.retry();

        assert_matches!(batch.next(), BatchResult::Item(3));
        assert_matches!(batch.next(), BatchResult::Item(4));
        assert_matches!(batch.next(), BatchResult::Complete); // max_size
    }

    #[test]
    fn test_batch_max_duration() {
        let mut batch = TxBatch::with_producer_thread(2, Duration::from_millis(100), |sender| {
            sender.send(Command::Append(1)).unwrap();
            std::thread::sleep(Duration::from_millis(500));
        });

        assert_matches!(batch.next(), BatchResult::Item(1));
        assert_matches!(batch.next(), BatchResult::Complete); // max_size
    }

    #[test]
    fn test_batch_disconnected() {
        let mut batch = TxBatch::with_producer_thread(2, Duration::from_secs(10), |sender| {
            sender.send(Command::Append(1)).unwrap();
        });

        assert_matches!(batch.next(), BatchResult::Item(1));
        assert_matches!(batch.next(), BatchResult::Complete); // disconnected

        assert!(batch.commit().is_err()); // disconnected
    }

    #[test]
    fn test_batch_command_complete() {
        let mut batch = TxBatch::with_producer_thread(2, Duration::from_secs(10), |sender| {
            sender.send(Command::Append(1)).unwrap();
            sender.send(Command::Complete).unwrap();
            sender.send(Command::Append(2)).unwrap();
            sender.send(Command::Complete).unwrap();
        });

        assert_matches!(batch.next(), BatchResult::Item(1));
        assert_matches!(batch.next(), BatchResult::Complete); // Command::Complete

        batch.commit().unwrap();

        assert_matches!(batch.next(), BatchResult::Item(2));
        assert_matches!(batch.next(), BatchResult::Complete); // Command::Complete
    }
}

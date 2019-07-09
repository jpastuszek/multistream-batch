//! `TxBatch` allows for batching incoming stream of items based on batch maximum size or maximum
//! duration since first item received. It also buffers the items so that current batch can be
//! processed again for example in case of downstream transaction failure.
use crossbeam_channel::Sender;
use crate::EndOfStreamError;
use crate::buf_batch::{BatchResult, BufBatchChannel, Command};

use std::fmt::Debug;
use std::time::Duration;
use std::vec::Drain;

// TODO: impl TxIterator that represents batch; when dropped batch is commited, has retry()
// function to start iteration from begginging of the batch

#[derive(Debug)]
pub struct TxBatch<I: Debug> {
    batch: BufBatchChannel<I>,
    // Retry uncommited number of messages before fetching next one/complete
    retry: Option<usize>,
}

impl<I: Debug> TxBatch<I> {
    pub fn new(max_size: usize, max_duration: Duration) -> (Sender<Command<I>>, TxBatch<I>) {
        let (sender, buf_batch) = BufBatchChannel::new(max_size, max_duration);
        (sender, TxBatch {
            batch: buf_batch,
            retry: None,
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
    /// * `max_duration` since first element returned elapsed,
    /// * client sent `Command::Complete`.
    /// 
    /// Caller is responsible for calling `retry` or `clear` (or other batch consuming methods) after receiving `BatchResult::Complete`.
    ///
    /// This call will block indefinitely waiting for first item of the batch.
    ///
    /// After calling `retry` this method will provide batch items again from the first one. It will
    /// continue fetching items from producer until max_size or max_duration is reached starting
    /// with original first item time.
    ///
    /// After calling `clear` this function will behave as if new `Batch` object was crated.
    pub fn next(&mut self) -> Result<BatchResult<I>, EndOfStreamError> {
        // Yield internal messages if batch was retried
        if let Some(retry) = self.retry {
            let item = &self.batch.as_slice()[self.batch.as_slice().len() - retry];
            if retry == 1 {
                self.retry = None;
            } else {
                self.retry = Some(retry - 1);
            }
            return Ok(BatchResult::Item(item))
        }

        self.batch.next()
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

    /// Retry batch making `next` to iterate already collected batch items starting from oldest one.
    pub fn retry(&mut self) {
        self.retry = Some(self.as_slice().len());
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    use std::time::Duration;
    use assert_matches::assert_matches;

    #[test]
    fn test_batch_retry() {
        let (sender, mut batch) = TxBatch::new(4, Duration::from_secs(10));

        sender.send(Command::Append(1)).unwrap();
        sender.send(Command::Append(2)).unwrap();
        sender.send(Command::Append(3)).unwrap();
        sender.send(Command::Append(4)).unwrap();
        sender.send(Command::Append(5)).unwrap();

        assert_matches!(batch.next(), Ok(BatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(BatchResult::Item(2)));
        assert_matches!(batch.next(), Ok(BatchResult::Item(3)));

        batch.retry();

        assert_matches!(batch.next(), Ok(BatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(BatchResult::Item(2)));
        assert_matches!(batch.next(), Ok(BatchResult::Item(3)));

        batch.retry();

        assert_matches!(batch.next(), Ok(BatchResult::Item(1)));

        batch.retry();

        assert_matches!(batch.next(), Ok(BatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(BatchResult::Item(2)));
        assert_matches!(batch.next(), Ok(BatchResult::Item(3)));
        assert_matches!(batch.next(), Ok(BatchResult::Item(4)));
        assert_matches!(batch.next(), Ok(BatchResult::Complete)); // max_size
    }

    #[test]
    fn test_batch_clear() {
        let (sender, mut batch) = TxBatch::new(2, Duration::from_secs(10));

        sender.send(Command::Append(1)).unwrap();
        sender.send(Command::Append(2)).unwrap();
        sender.send(Command::Append(3)).unwrap();
        sender.send(Command::Append(4)).unwrap();

        assert_matches!(batch.next(), Ok(BatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(BatchResult::Item(2)));
        assert_matches!(batch.next(), Ok(BatchResult::Complete)); // max_size

        batch.clear();

        assert_matches!(batch.next(), Ok(BatchResult::Item(3)));

        batch.retry();

        assert_matches!(batch.next(), Ok(BatchResult::Item(3)));
        assert_matches!(batch.next(), Ok(BatchResult::Item(4)));
        assert_matches!(batch.next(), Ok(BatchResult::Complete)); // max_size
    }

    #[test]
    fn test_batch_with_producer_thread() {
        let mut batch = TxBatch::with_producer_thread(2, Duration::from_secs(10), |sender| {
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

        batch.retry();

        assert_matches!(batch.next(), Ok(BatchResult::Item(3)));
        assert_matches!(batch.next(), Ok(BatchResult::Item(4)));
        assert_matches!(batch.next(), Ok(BatchResult::Complete)); // max_size
    }

    #[test]
    fn test_batch_max_duration() {
        let mut batch = TxBatch::with_producer_thread(2, Duration::from_millis(100), |sender| {
            sender.send(Command::Append(1)).unwrap();
            std::thread::sleep(Duration::from_millis(500));
        });

        assert_matches!(batch.next(), Ok(BatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(BatchResult::Complete)); // max_size
    }

    #[test]
    fn test_batch_disconnected() {
        let mut batch = TxBatch::with_producer_thread(2, Duration::from_secs(10), |sender| {
            sender.send(Command::Append(1)).unwrap();
        });

        assert_matches!(batch.next(), Ok(BatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(BatchResult::Complete)); // disconnected
        assert_matches!(batch.next(), Err(EndOfStreamError));
    }

    #[test]
    fn test_batch_command_complete() {
        let mut batch = TxBatch::with_producer_thread(2, Duration::from_secs(10), |sender| {
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

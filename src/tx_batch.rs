//! `TxBatch` allows for batching incoming stream of items based on batch maximum size or maximum
//! duration since first item received. It also buffers the items so that current batch can be
//! processed again for example in case of downstream transaction failure.
use crossbeam_channel::{Sender, Receiver, RecvTimeoutError};
use crate::EndOfStreamError;
use crate::buf_batch::{BufBatch, PollResult};

use std::fmt::Debug;
use std::time::Duration;
use std::vec::Drain;

/// Commands that can be send to `TxBatch` via `Sender` endpoint.
#[derive(Debug)]
pub enum Command<I: Debug> {
    /// Append item `I` to batch.
    Append(I),
    /// Flush outstanding items.
    Complete,
}

/// Provides actions that can be taken to consume complete batch.
#[derive(Debug)]
pub struct Complete<'i, I: Debug>(&'i mut TxBatch<I>);

impl<'i, I: Debug> Complete<'i, I> {
    /// Restarts batch making `TxBatch.next()` to iterate already appended items starting from oldest one in current batch.
    pub fn retry(&mut self) {
        self.0.retry()
    }

    /// Commits current batch by dropping all buffered items.
    pub fn commit(&mut self) {
        self.0.commit()
    }

    /// Commits current batch by drainig all buffered items.
    pub fn drain(&mut self) -> Drain<I> {
        self.0.drain()
    }
}

/// Result of batching operation
#[derive(Debug)]
pub enum TxBatchResult<'i, I: Debug> {
    /// New item appended to batch
    Item(&'i I),
    /// Batch is now complete
    Complete(Complete<'i, I>),
}

/// Collect items into batches while simultaniously processing them in a transaction (e.g. insert into DB). 
/// When batch is complete transaction can be commited (e.g. DB COMMIT).
/// 
/// If transaction succeeded batch can be cleared with `self.commit()` and new transaction started.
/// In case transaction failed (e.g. due to confilct) batch can be retried with `self.retry()` in new transaction.
#[derive(Debug)]
pub struct TxBatch<I: Debug> {
    channel: Receiver<Command<I>>,
    batch: BufBatch<I>,
    // Retry uncommited number of messages before fetching next one/complete
    retry: Option<usize>,
    // True when channel is disconnected
    disconnected: bool,
}

impl<I: Debug> TxBatch<I> {
    /// Creates batch given maximum batch size in number of items (`max_size`)
    /// and maximum duration that batch can last (`max_duration`) since first item appended to it. 
    pub fn new(max_size: usize, max_duration: Duration) -> (Sender<Command<I>>, TxBatch<I>) {
        let (sender, receiver) = crossbeam_channel::bounded(max_size * 2);

        (sender, TxBatch {
            channel: receiver,
            batch: BufBatch::new(max_size, max_duration),
            retry: None,
            disconnected: false,
        })
    }

    /// Crates batch calling `producer` closure with `Sender` end of the channel in newly started thread.
    pub fn with_producer_thread(max_size: usize, max_duration: Duration, producer: impl Fn(Sender<Command<I>>) -> () + Send + 'static) -> TxBatch<I> where I: Send + 'static {
        let (sender, batch) = TxBatch::new(max_size, max_duration);

        std::thread::spawn(move || {
            producer(sender)
        });

        batch
    }

    /// Gets next item from the batch.
    ///
    /// Returns `Ok(TxBatchResult::Item(I))` with next item of the batch.
    ///
    /// Returns `Ok(TxBatchResult::Complete(complete))` signaling end of batch if:
    /// * `max_size` of the batch was reached,
    /// * `max_duration` since first element returned elapsed,
    /// * client sent `Command::Complete`.
    /// 
    /// Caller is responsible for calling `retry` or `clear` (or other batch consuming methods) after receiving `TxBatchResult::Complete`.
    ///
    /// This call will block indefinitely waiting for first item of the batch.
    ///
    /// After calling `retry` this method will provide batch items again from the first one. It will
    /// continue fetching items from producer until max_size or max_duration is reached starting
    /// with original first item time.
    ///
    /// After calling `clear` this function will behave as if new `Batch` object was crated.
    pub fn next(&mut self) -> Result<TxBatchResult<I>, EndOfStreamError> {
        // Yield internal messages if batch was retried
        if let Some(retry) = self.retry {
            let item = &self.batch.as_slice()[self.batch.as_slice().len() - retry];
            if retry == 1 {
                self.retry = None;
            } else {
                self.retry = Some(retry - 1);
            }
            return Ok(TxBatchResult::Item(item))
        }

        if self.disconnected {
            return Err(EndOfStreamError)
        }

        loop {
            // Check if we have a ready batch due to any limit or go fetch next item
            let ready_after = match self.batch.poll() {
                PollResult::Ready => return Ok(TxBatchResult::Complete(Complete(self))),
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
                    return Ok(TxBatchResult::Item(item))
                }
                Ok(Command::Complete) => {
                    // Mark as complete by producer
                    return Ok(TxBatchResult::Complete(Complete(self)))
                },
                Err(_eos) => {
                    self.disconnected = true;
                    return Ok(TxBatchResult::Complete(Complete(self)))
                }
            };
        }
    }

    /// Checks if previous `self.next()` call found channel to be disconnected.
    pub fn is_disconnected(&self) -> bool {
        self.disconnected
    }

    /// Restarts batch making `self.next()` to iterate already appended items starting from oldest one in current batch.
    pub fn retry(&mut self) {
        self.retry = Some(self.as_slice().len());
    }

    /// Commits current batch by calling `self.take().clear()`.
    pub fn commit(&mut self) {
        self.batch.clear()
    }

    /// Consumes batch by draining items from internal buffer.
    pub fn drain(&mut self) -> Drain<I> {
        self.batch.drain()
    }

    /// Converts into internal item buffer.
    pub fn into_vec(self) -> Vec<I> {
        self.batch.into_vec()
    }

    /// Returns slice of internal item buffer.
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
    fn test_batch_retry() {
        let (sender, mut batch) = TxBatch::new(4, Duration::from_secs(10));

        sender.send(Command::Append(1)).unwrap();
        sender.send(Command::Append(2)).unwrap();
        sender.send(Command::Append(3)).unwrap();
        sender.send(Command::Append(4)).unwrap();
        sender.send(Command::Append(5)).unwrap();

        assert_matches!(batch.next(), Ok(TxBatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Item(3)));

        batch.retry();

        assert_matches!(batch.next(), Ok(TxBatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Item(3)));

        batch.retry();

        assert_matches!(batch.next(), Ok(TxBatchResult::Item(1)));

        batch.retry();

        assert_matches!(batch.next(), Ok(TxBatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Item(3)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Item(4)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Complete(mut complete)) => complete.retry()); // max_size

        assert_matches!(batch.next(), Ok(TxBatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Item(3)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Item(4)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Complete(mut complete)) => complete.commit()); // max_size

        sender.send(Command::Append(5)).unwrap();
        assert_matches!(batch.next(), Ok(TxBatchResult::Item(5)));
    }

    #[test]
    fn test_batch_commit() {
        let (sender, mut batch) = TxBatch::new(2, Duration::from_secs(10));

        sender.send(Command::Append(1)).unwrap();
        sender.send(Command::Append(2)).unwrap();
        sender.send(Command::Append(3)).unwrap();
        sender.send(Command::Append(4)).unwrap();

        assert_matches!(batch.next(), Ok(TxBatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Complete(_complete)));

        batch.commit();

        assert_matches!(batch.next(), Ok(TxBatchResult::Item(3)));

        batch.retry();

        assert_matches!(batch.next(), Ok(TxBatchResult::Item(3)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Item(4)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Complete(mut complete)) =>
            assert_eq!(complete.drain().collect::<Vec<_>>().as_slice(), [3, 4])
        ); // max_size
    }

    #[test]
    fn test_batch_with_producer_thread() {
        let mut batch = TxBatch::with_producer_thread(2, Duration::from_secs(10), |sender| {
            sender.send(Command::Append(1)).unwrap();
            sender.send(Command::Append(2)).unwrap();
            sender.send(Command::Append(3)).unwrap();
            sender.send(Command::Append(4)).unwrap();
        });

        assert_matches!(batch.next(), Ok(TxBatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Complete(_complete)));

        batch.commit();

        assert_matches!(batch.next(), Ok(TxBatchResult::Item(3)));

        batch.retry();

        assert_matches!(batch.next(), Ok(TxBatchResult::Item(3)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Item(4)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Complete(mut complete)) =>
            assert_eq!(complete.drain().collect::<Vec<_>>().as_slice(), [3, 4])
        ); // max_size
    }

    #[test]
    fn test_batch_max_duration() {
        let mut batch = TxBatch::with_producer_thread(2, Duration::from_millis(100), |sender| {
            sender.send(Command::Append(1)).unwrap();
            std::thread::sleep(Duration::from_millis(500));
        });

        assert_matches!(batch.next(), Ok(TxBatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Complete(mut complete)) =>
            assert_eq!(complete.drain().collect::<Vec<_>>().as_slice(), [1])
        ); // max_duration
    }

    #[test]
    fn test_batch_disconnected() {
        let mut batch = TxBatch::with_producer_thread(2, Duration::from_secs(10), |sender| {
            sender.send(Command::Append(1)).unwrap();
        });

        assert_matches!(batch.next(), Ok(TxBatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Complete(mut complete)) =>
            assert_eq!(complete.drain().collect::<Vec<_>>().as_slice(), [1])
        ); // disconnected
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

        assert_matches!(batch.next(), Ok(TxBatchResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Complete(_complete)));

        batch.commit();

        assert_matches!(batch.next(), Ok(TxBatchResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBatchResult::Complete(mut complete)) =>
            assert_eq!(complete.drain().collect::<Vec<_>>().as_slice(), [2])
        ); // command
    }
}

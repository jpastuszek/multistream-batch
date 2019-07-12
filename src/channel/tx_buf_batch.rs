//! This module provides `TxBufBatchChannel` that will produce references to stored items as soon as
//! they are received. The batch will signal when it is ready due to reaching one of its limits at
//! which point it can be committed or retried.
//! This implementation is using `crossbeam_channel` to implement awaiting for items or timeout.
use crossbeam_channel::{Sender, Receiver, RecvTimeoutError};
use crate::channel::EndOfStreamError;
use crate::buf_batch::{BufBatch, PollResult};

use std::fmt::Debug;
use std::time::Duration;
use std::vec::Drain;

/// Commands that can be send to `TxBufBatchChannel` via `Sender` endpoint.
#[derive(Debug)]
pub enum Command<I: Debug> {
    /// Append item `I` to batch.
    Append(I),
    /// Flush outstanding items.
    Flush,
}

/// Provides actions that can be taken to consume complete batch.
#[derive(Debug)]
pub struct Complete<'i, I: Debug>(&'i mut TxBufBatchChannel<I>);

impl<'i, I: Debug> Complete<'i, I> {
    /// Restarts batch making `TxBufBatchChannel.next()` to iterate already received items starting from oldest one in current batch.
    pub fn retry(&mut self) {
        self.0.retry()
    }

    /// Commits current batch by dropping all buffered items.
    pub fn commit(&mut self) {
        self.0.clear()
    }

    /// Commits current batch by draining all buffered items.
    pub fn drain(&mut self) -> Drain<I> {
        self.0.drain()
    }
}

/// Represents result from `TxBufBatchChannel.next()` function call.
#[derive(Debug)]
pub enum TxBufBatchChannelResult<'i, I: Debug> {
    /// New item appended to batch
    Item(&'i I),
    /// Batch is now complete
    Complete(Complete<'i, I>),
}

/// Batches items in internal buffer up to `max_size` items or until `max_duration` has elapsed
/// since first item was appended to the batch. Reference to each item is returned for every
/// received item as soon as they are received.
///
/// This batch can provide all the buffered item references in order as they were received again
/// after batch was completed but retried (not committed).
///
/// This implementation is using `crossbeam_channel` to implement awaiting for items or timeout.
#[derive(Debug)]
pub struct TxBufBatchChannel<I: Debug> {
    channel: Receiver<Command<I>>,
    batch: BufBatch<I>,
    // Retry uncommited number of messages before fetching next one/complete
    retry: Option<usize>,
    // True when channel is disconnected
    disconnected: bool,
}

impl<I: Debug> TxBufBatchChannel<I> {
    /// Creates batch given maximum batch size in number of items (`max_size`)
    /// and maximum duration that batch can last (`max_duration`) since first item appended to it.
    ///
    /// Panics if `max_size` == 0.
    pub fn new(max_size: usize, max_duration: Duration, channel_size: usize) -> (Sender<Command<I>>, TxBufBatchChannel<I>) {
        let (sender, receiver) = crossbeam_channel::bounded(channel_size);

        (sender, TxBufBatchChannel {
            channel: receiver,
            batch: BufBatch::new(max_size, max_duration),
            retry: None,
            disconnected: false,
        })
    }

    /// Crates batch calling `producer` closure with `Sender` end of the channel in newly started thread.
    pub fn with_producer_thread(max_size: usize, max_duration: Duration, channel_size: usize, producer: impl FnOnce(Sender<Command<I>>) -> () + Send + 'static) -> TxBufBatchChannel<I> where I: Send + 'static {
        let (sender, batch) = TxBufBatchChannel::new(max_size, max_duration, channel_size);

        std::thread::spawn(move || {
            producer(sender)
        });

        batch
    }

    /// Gets next item reference received by the batch or signal that the batch is now complete and
    /// can be retried or committed.
    ///
    /// This call will block until batch becomes ready.
    ///
    /// Returns `Err(EndOfStreamError)` after `Sender` end was dropped and all batched items were flushed.
    pub fn next(&mut self) -> Result<TxBufBatchChannelResult<I>, EndOfStreamError> {
        // Yield internal messages if batch was retried
        if let Some(retry) = self.retry {
            let item = &self.batch.as_slice()[self.batch.as_slice().len() - retry];
            if retry == 1 {
                self.retry = None;
            } else {
                self.retry = Some(retry - 1);
            }
            return Ok(TxBufBatchChannelResult::Item(item))
        }

        if self.disconnected {
            return Err(EndOfStreamError)
        }

        loop {
            // Check if we have a ready batch due to any limit or go fetch next item
            let ready_after = match self.batch.poll() {
                PollResult::Ready => return Ok(TxBufBatchChannelResult::Complete(Complete(self))),
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
                    return Ok(TxBufBatchChannelResult::Item(item))
                }
                Ok(Command::Flush) => {
                    // Mark as complete by producer
                    return Ok(TxBufBatchChannelResult::Complete(Complete(self)))
                },
                Err(_eos) => {
                    self.disconnected = true;
                    return Ok(TxBufBatchChannelResult::Complete(Complete(self)))
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

    /// Starts new batch dropping all buffered items.
    pub fn clear(&mut self) {
        self.batch.clear()
    }

    /// Starts new batch by draining all buffered items.
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
        let (sender, mut batch) = TxBufBatchChannel::new(4, Duration::from_secs(10), 10);

        sender.send(Command::Append(1)).unwrap();
        sender.send(Command::Append(2)).unwrap();
        sender.send(Command::Append(3)).unwrap();
        sender.send(Command::Append(4)).unwrap();
        sender.send(Command::Append(5)).unwrap();

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));

        batch.retry();

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));

        batch.retry();

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));

        batch.retry();

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(4)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Complete(mut complete)) => complete.retry()); // max_size

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(4)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Complete(_))); // max_size

        batch.retry();

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(4)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Complete(mut complete)) => complete.commit()); // max_size

        sender.send(Command::Append(5)).unwrap();
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(5)));
    }

    #[test]
    fn test_batch_commit() {
        let (sender, mut batch) = TxBufBatchChannel::new(2, Duration::from_secs(10), 10);

        sender.send(Command::Append(1)).unwrap();
        sender.send(Command::Append(2)).unwrap();
        sender.send(Command::Append(3)).unwrap();
        sender.send(Command::Append(4)).unwrap();

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Complete(_complete)));

        batch.clear();

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));

        batch.retry();

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(4)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Complete(mut complete)) =>
            assert_eq!(complete.drain().collect::<Vec<_>>().as_slice(), [3, 4])
        ); // max_size
    }

    #[test]
    fn test_batch_with_producer_thread() {
        let mut batch = TxBufBatchChannel::with_producer_thread(2, Duration::from_secs(10), 10, |sender| {
            sender.send(Command::Append(1)).unwrap();
            sender.send(Command::Append(2)).unwrap();
            sender.send(Command::Append(3)).unwrap();
            sender.send(Command::Append(4)).unwrap();
        });

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Complete(_complete)));

        batch.clear();

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));

        batch.retry();

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(4)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Complete(mut complete)) =>
            assert_eq!(complete.drain().collect::<Vec<_>>().as_slice(), [3, 4])
        ); // max_size
    }

    #[test]
    fn test_batch_max_duration() {
        let mut batch = TxBufBatchChannel::with_producer_thread(2, Duration::from_millis(100), 10, |sender| {
            sender.send(Command::Append(1)).unwrap();
            std::thread::sleep(Duration::from_millis(500));
        });

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Complete(mut complete)) =>
            assert_eq!(complete.drain().collect::<Vec<_>>().as_slice(), [1])
        ); // max_duration
    }

    #[test]
    fn test_batch_disconnected() {
        let mut batch = TxBufBatchChannel::with_producer_thread(2, Duration::from_secs(10), 10, |sender| {
            sender.send(Command::Append(1)).unwrap();
        });

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Complete(mut complete)) =>
            assert_eq!(complete.drain().collect::<Vec<_>>().as_slice(), [1])
        ); // disconnected
        assert_matches!(batch.next(), Err(EndOfStreamError));
    }

    #[test]
    fn test_batch_command_complete() {
        let mut batch = TxBufBatchChannel::with_producer_thread(2, Duration::from_secs(10), 10, |sender| {
            sender.send(Command::Append(1)).unwrap();
            sender.send(Command::Flush).unwrap();
            sender.send(Command::Append(2)).unwrap();
            sender.send(Command::Flush).unwrap();
        });

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Complete(_complete)));

        batch.clear();

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Complete(mut complete)) =>
            assert_eq!(complete.drain().collect::<Vec<_>>().as_slice(), [2])
        ); // command
    }
}

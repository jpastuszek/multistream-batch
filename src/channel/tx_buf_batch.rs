/*!
This module provides `TxBufBatchChannel` that will produce references to stored items as soon as
they are received. The batch will signal when it is ready due to reaching one of its limits at
which point it can be committed or retried.

# Example

```rust
use multistream_batch::channel::tx_buf_batch::TxBufBatchChannel;
use multistream_batch::channel::tx_buf_batch::Command::*;
use multistream_batch::channel::tx_buf_batch::TxBufBatchChannelResult::*;
use std::time::Duration;
use assert_matches::assert_matches;

// Create producer thread and batcher with maximum size of 4 items and
// maximum batch duration since first received item of 200 ms.
let mut batch = TxBufBatchChannel::with_producer_thread(4, Duration::from_millis(200), 10, |sender| {
    // Send a sequence of `Append` commands with integer item value
    sender.send(Append(1)).unwrap();
    sender.send(Append(2)).unwrap();
    sender.send(Append(3)).unwrap();
    sender.send(Append(4)).unwrap();
    // At this point batch should have reached its capacity of 4 items

    // Send some more to buffer up for next batch
    sender.send(Append(5)).unwrap();
    sender.send(Append(6)).unwrap();

    // Introduce delay to trigger maximum duration timeout
    std::thread::sleep(Duration::from_millis(400));

    // Send items that will be flushed by `Flush` command
    sender.send(Append(7)).unwrap();
    sender.send(Append(8)).unwrap();
    // Flush outstanding items
    sender.send(Flush).unwrap();

    // Last buffered up items will be flushed automatically when this thread exits
    sender.send(Append(9)).unwrap();
    sender.send(Append(10)).unwrap();
    // Exiting closure will shutdown the producer thread
});

// This batch will provide reference to each stored item as soon as it is sent
assert_matches!(batch.next(), Ok(Item(1)));
assert_matches!(batch.next(), Ok(Item(2)));
assert_matches!(batch.next(), Ok(Item(3)));

// Now will will retry the batch before it is complete
batch.retry();

// Items are provided again from the oldest one of the batch
assert_matches!(batch.next(), Ok(Item(1)));
assert_matches!(batch.next(), Ok(Item(2)));
assert_matches!(batch.next(), Ok(Item(3)));
assert_matches!(batch.next(), Ok(BufferedComplete(_)));
assert_matches!(batch.next(), Ok(Item(4)));

// Batch flushed due to size limit; call `.retry()` once more on the `Complete` result
assert_matches!(batch.next(), Ok(Complete(mut complete)) => complete.retry());

// Items are provided again from the oldest one of the batch
assert_matches!(batch.next(), Ok(Item(1)));
assert_matches!(batch.next(), Ok(Item(2)));
assert_matches!(batch.next(), Ok(Item(3)));
assert_matches!(batch.next(), Ok(Item(4)));
assert_matches!(batch.next(), Ok(BufferedComplete(_)));
// Batch flushed due to size limit; call `.commit()` on the `Complete` result to start new batch
assert_matches!(batch.next(), Ok(Complete(mut complete)) => complete.commit());

assert_matches!(batch.next(), Ok(Item(5)));
assert_matches!(batch.next(), Ok(Item(6)));
// Batch flushed due to duration limit
assert_matches!(batch.next(), Ok(Complete(mut complete)) => complete.commit());

assert_matches!(batch.next(), Ok(Item(7)));
assert_matches!(batch.next(), Ok(Item(8)));
// Batch flushed by sending `Flush` command
assert_matches!(batch.next(), Ok(Complete(mut complete)) => complete.commit());

assert_matches!(batch.next(), Ok(Item(9)));
assert_matches!(batch.next(), Ok(Item(10)));
// Batch flushed by dropping sender (thread exit)
assert_matches!(batch.next(), Ok(Complete(mut complete)) => complete.commit());
```
!*/

use crate::buf_batch::{BufBatch, PollResult};
use crate::channel::EndOfStreamError;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};

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
    /// Batch retry is now complete
    BufferedComplete(Complete<'i, I>),
}

/// Batches items in internal buffer up to `max_size` items or until `max_duration` has elapsed
/// since first item was appended to the batch. Reference to each item is returned for every
/// received item as soon as they are received.
///
/// This batch can provide all the buffered item references in order as they were received again
/// after batch was completed but retried (not committed).
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
    pub fn new(
        max_size: usize,
        max_duration: Duration,
        channel_size: usize,
    ) -> (Sender<Command<I>>, TxBufBatchChannel<I>) {
        let (sender, receiver) = crossbeam_channel::bounded(channel_size);

        (
            sender,
            TxBufBatchChannel {
                channel: receiver,
                batch: BufBatch::new(max_size, max_duration),
                retry: None,
                disconnected: false,
            },
        )
    }

    /// Crates batch calling `producer` closure with `Sender` end of the channel in newly started thread.
    pub fn with_producer_thread(
        max_size: usize,
        max_duration: Duration,
        channel_size: usize,
        producer: impl FnOnce(Sender<Command<I>>) -> () + Send + 'static,
    ) -> TxBufBatchChannel<I>
    where
        I: Send + 'static,
    {
        let (sender, batch) = TxBufBatchChannel::new(max_size, max_duration, channel_size);

        std::thread::Builder::new().name("TxBufBatchChannel producer".to_string()).spawn(move || producer(sender)).expect("failed to start producer thread");

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
            if retry == 0 {
                self.retry = None;
                return Ok(TxBufBatchChannelResult::BufferedComplete(Complete(self)));
            }

            let item = &self.batch.as_slice()[self.batch.as_slice().len() - retry];

            self.retry = Some(retry - 1);
            return Ok(TxBufBatchChannelResult::Item(item));
        }

        if self.disconnected {
            return Err(EndOfStreamError);
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
                    return Ok(TxBufBatchChannelResult::Item(item));
                }
                Ok(Command::Flush) => {
                    // Mark as complete by producer
                    return Ok(TxBufBatchChannelResult::Complete(Complete(self)));
                }
                Err(_eos) => {
                    self.disconnected = true;
                    return Ok(TxBufBatchChannelResult::Complete(Complete(self)));
                }
            };
        }
    }

    /// Pops last item returned by `next` from internal buffer - it won't be retried.
    ///
    /// Can be called multiple times to consume the internal buffer starting at last `next` item
    /// position.
    pub fn pop(&mut self) -> Option<I> {
        if let Some(retry) = self.retry {
            // just after `retry()`; iteration not started yet
            if retry == self.batch.as_slice().len() {
                return None;
            }

            // remove from before retry pointer (retry + 1), retry will now point to the previous
            // item correctly
            return  Some(self.batch.remove(self.batch.as_slice().len() - (retry + 1)));
        }
        self.batch.pop()
    }

    /// Checks if previous `self.next()` call found channel to be disconnected.
    pub fn is_disconnected(&self) -> bool {
        self.disconnected
    }

    /// Restarts batch making `self.next()` to iterate already appended items starting from oldest one in current batch.
    ///
    /// Panics if batch is empty.
    pub fn retry(&mut self) {
        assert!(self.as_slice().len() > 0, "TxBufBatchChannel retry of empty batch");
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

    /// Returns slice of internal item buffer.
    pub fn as_slice(&self) -> &[I] {
        self.batch.as_slice()
    }

    /// Converts into internal item buffer.
    pub fn into_vec(self) -> Vec<I> {
        self.batch.into_vec()
    }

    /// Converts to an iterator that will drain all buffered items first and then all items from the channel.
    pub fn drain_to_end(self) -> DrainToEnd<I> {
        let (buffer, channel) = self.split();
        DrainToEnd(buffer.into_vec().into_iter(), channel)
    }

    /// Splits into `BufBatch` item buffer and channel `Receiver` end
    pub fn split(self) -> (BufBatch<I>, Receiver<Command<I>>) {
        (self.batch, self.channel)
    }
}

/// Iterator that will drain all buffered items first and then all items from the channel.
#[derive(Debug)]
pub struct DrainToEnd<I: Debug>(std::vec::IntoIter<I>, Receiver<Command<I>>);

impl<I: Debug> Iterator for DrainToEnd<I> {
    type Item = I;

    fn next(&mut self) -> Option<I> {
        self.0.next().or_else(|| {
            loop {
                match self.1.recv() {
                    Ok(Command::Append(i)) => return Some(i),
                    Ok(Command::Flush) => (),
                    Err(_) => return None
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    use assert_matches::assert_matches;
    use std::time::Duration;

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
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::BufferedComplete(_))); // all items in buffer processed
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(4)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Complete(mut complete)) => complete.retry()); // max_size

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(4)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::BufferedComplete(_)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Complete(_))); // max_size

        batch.retry();

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(4)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::BufferedComplete(_)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Complete(mut complete)) => complete.commit()); // max_size

        sender.send(Command::Append(5)).unwrap();
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(5)));
    }

    #[test]
    #[should_panic]
    fn test_batch_empty() {
        let mut batch =
            TxBufBatchChannel::with_producer_thread(2, Duration::from_millis(100), 10, |sender| {
                sender.send(Command::Append(1)).unwrap();
                std::thread::sleep(Duration::from_millis(500));
            });

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));

        batch.clear();
        batch.retry();

        assert_matches!(batch.next(), Err(EndOfStreamError));
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
        assert_matches!(
            batch.next(),
            Ok(TxBufBatchChannelResult::Complete(_complete))
        );

        batch.clear();

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));

        batch.retry();

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::BufferedComplete(mut complete)) => complete.commit());
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(4)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Complete(mut complete)) =>
            assert_eq!(complete.drain().collect::<Vec<_>>().as_slice(), [4])
        ); // max_size
    }

    #[test]
    fn test_batch_with_producer_thread() {
        let mut batch =
            TxBufBatchChannel::with_producer_thread(2, Duration::from_secs(10), 10, |sender| {
                sender.send(Command::Append(1)).unwrap();
                sender.send(Command::Append(2)).unwrap();
                sender.send(Command::Append(3)).unwrap();
                sender.send(Command::Append(4)).unwrap();
            });

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(2)));
        assert_matches!(
            batch.next(),
            Ok(TxBufBatchChannelResult::Complete(_complete))
        );

        batch.clear();

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));

        batch.retry();

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::BufferedComplete(mut complete)) => complete.commit());
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(4)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Complete(mut complete)) =>
            assert_eq!(complete.drain().collect::<Vec<_>>().as_slice(), [4])
        ); // max_size
    }

    #[test]
    fn test_batch_max_duration() {
        let mut batch =
            TxBufBatchChannel::with_producer_thread(2, Duration::from_millis(100), 10, |sender| {
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
        let mut batch =
            TxBufBatchChannel::with_producer_thread(2, Duration::from_secs(10), 10, |sender| {
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
        let mut batch =
            TxBufBatchChannel::with_producer_thread(2, Duration::from_secs(10), 10, |sender| {
                sender.send(Command::Append(1)).unwrap();
                sender.send(Command::Flush).unwrap();
                sender.send(Command::Append(2)).unwrap();
                sender.send(Command::Flush).unwrap();
            });

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));
        assert_matches!(
            batch.next(),
            Ok(TxBufBatchChannelResult::Complete(_complete))
        );

        batch.clear();

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Complete(mut complete)) =>
            assert_eq!(complete.drain().collect::<Vec<_>>().as_slice(), [2])
        ); // command
    }

    #[test]
    fn test_drain_to_end() {
        let (sender, mut batch) = TxBufBatchChannel::new(4, Duration::from_secs(10), 10);

        sender.send(Command::Append(1)).unwrap();
        sender.send(Command::Append(2)).unwrap();
        sender.send(Command::Append(3)).unwrap();
        sender.send(Command::Append(4)).unwrap();
        sender.send(Command::Append(5)).unwrap();
        drop(sender);

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(2)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));

        batch.retry();

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));

        assert_eq!(batch.drain_to_end().collect::<Vec<_>>().as_slice(), [1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_pop() {
        let (sender, mut batch) = TxBufBatchChannel::new(40, Duration::from_secs(10), 10);

        sender.send(Command::Append(1)).unwrap();
        sender.send(Command::Append(2)).unwrap();
        sender.send(Command::Append(3)).unwrap();
        sender.send(Command::Append(4)).unwrap();
        sender.send(Command::Append(5)).unwrap();
        sender.send(Command::Append(6)).unwrap();
        sender.send(Command::Append(7)).unwrap();
        drop(sender);

        assert_matches!(batch.pop(), None);
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(2)));
        assert_matches!(batch.pop(), Some(2));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(4)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(5)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(6)));
        assert_matches!(batch.pop(), Some(6));
        assert_matches!(batch.pop(), Some(5));

        batch.retry();
        assert_matches!(batch.pop(), None);

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(1)));
        assert_matches!(batch.pop(), Some(1));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(4)));

        batch.retry();
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(3)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(4)));
        assert_matches!(batch.pop(), Some(4));
        assert_matches!(batch.pop(), Some(3));

        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::BufferedComplete(_)));
        assert_matches!(batch.next(), Ok(TxBufBatchChannelResult::Item(7)));
    }
}

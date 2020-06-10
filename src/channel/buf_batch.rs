/*!
This module provides `BufBatchChannel` that will buffer items until the batch is ready and provide them in
one go using `Drain` iterator.

# Example

Collect batches of items after reaching different limits and use of `Flush` command.

```rust
use multistream_batch::channel::buf_batch::BufBatchChannel;
use multistream_batch::channel::buf_batch::Command::*;
use std::time::Duration;
use assert_matches::assert_matches;

// Create producer thread with `BufBatchChannel` configured with a maximum size of 4 items and
// a maximum batch duration since the first received item of 200 ms.
let mut batch = BufBatchChannel::with_producer_thread(4, Duration::from_millis(200), 10, |sender| {
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

// Batch flushed due to size limit
assert_matches!(batch.next(), Ok(drain) =>
    assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
);

// Batch flushed due to duration limit
assert_matches!(batch.next(), Ok(drain) =>
    assert_eq!(drain.collect::<Vec<_>>().as_slice(), [5, 6])
);

// Batch flushed by sending `Flush` command
assert_matches!(batch.next(), Ok(drain) =>
    assert_eq!(drain.collect::<Vec<_>>().as_slice(), [7, 8])
);

// Batch flushed by dropping sender (thread exit)
assert_matches!(batch.next(), Ok(drain) =>
    assert_eq!(drain.collect::<Vec<_>>().as_slice(), [9, 10])
);
```
!*/

use crate::buf_batch::{BufBatch, PollResult};
use crate::channel::EndOfStreamError;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};

use std::fmt::Debug;
use std::time::Duration;
use std::vec::Drain;

/// Commands that can be sent to `BufBatchChannel` via `Sender` endpoint.
#[derive(Debug)]
pub enum Command<I: Debug> {
    /// Append item `I` to batch.
    Append(I),
    /// Flush outstanding items.
    Flush,
}

/// Batches items in internal buffer up to `max_size` items or until `max_duration` has elapsed
/// since the first item appended to the batch.
#[derive(Debug)]
pub struct BufBatchChannel<I: Debug> {
    channel: Receiver<Command<I>>,
    batch: BufBatch<I>,
    // Whenever channel is disconnected
    disconnected: bool,
}

impl<I: Debug> BufBatchChannel<I> {
    /// Creates batch given maximum batch size in the number of items stored (`max_size`)
    /// and maximum duration that batch can last (`max_duration`) since the first item appended to it.
    /// Parameter `channel_size` defines the maximum number of messages that can be buffered between sender and receiver.
    ///
    /// This method also returns `Sender` endpoint that can be used to send `Command`s.
    ///
    /// Panics if `max_size == 0`.
    pub fn new(
        max_size: usize,
        max_duration: Duration,
        channel_size: usize,
    ) -> (Sender<Command<I>>, BufBatchChannel<I>) {
        let (sender, receiver) = crossbeam_channel::bounded(channel_size);

        (
            sender,
            BufBatchChannel {
                channel: receiver,
                batch: BufBatch::new(max_size, max_duration),
                disconnected: false,
            },
        )
    }

    /// Calls `producer` closure with `Sender` end of the channel in a newly started thread and
    /// returns `BufBatchChannel` connected to that `Sender`.
    pub fn with_producer_thread(
        max_size: usize,
        max_duration: Duration,
        channel_size: usize,
        producer: impl FnOnce(Sender<Command<I>>) -> () + Send + 'static,
    ) -> BufBatchChannel<I>
    where
        I: Send + 'static,
    {
        let (sender, batch) = BufBatchChannel::new(max_size, max_duration, channel_size);

        std::thread::Builder::new().name("BufBatchChannel producer".to_string()).spawn(move || producer(sender)).expect("failed to start producer thread");

        batch
    }

    /// Gets next ready batch as `Drain` iterator of its items.
    ///
    /// This call will block until batch becomes ready.
    ///
    /// When the `Sender` end has dropped, this method returns with `Err(EndOfStreamError)` after all
    /// outstanding items were flushed.
    pub fn next(&mut self) -> Result<Drain<I>, EndOfStreamError> {
        if self.disconnected {
            return Err(EndOfStreamError);
        }

        loop {
            // Check if we have a ready batch due to any limit or go fetch next item
            let ready_after = match self.batch.poll() {
                PollResult::Ready => return Ok(self.batch.drain()),
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
                Ok(Command::Flush) => {
                    // Mark as complete by producer
                    return Ok(self.batch.drain());
                }
                Ok(Command::Append(item)) => {
                    self.batch.append(item);
                    continue;
                }
                Err(_eos) => {
                    self.disconnected = true;
                    return Ok(self.batch.drain());
                }
            };
        }
    }

    /// Checks if previous `self.next()` call found the channel to be disconnected.
    pub fn is_disconnected(&self) -> bool {
        self.disconnected
    }

    /// Starts new batch dropping all buffered items.
    pub fn clear(&mut self) {
        self.batch.clear()
    }

    /// Returns slice of internal item buffer.
    pub fn as_slice(&self) -> &[I] {
        self.batch.as_slice()
    }

    /// Starts new batch by draining all buffered items.
    pub fn drain(&mut self) -> Drain<I> {
        self.batch.drain()
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

    /// Splits into `BufBatch` item buffer and channel `Receiver` end.
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
    fn test_batch_max_size() {
        let (sender, mut batch) = BufBatchChannel::new(2, Duration::from_secs(10), 10);

        sender.send(Command::Append(1)).unwrap();
        sender.send(Command::Append(2)).unwrap();
        sender.send(Command::Append(3)).unwrap();

        assert_matches!(batch.next(), Ok(drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        ); // max_size
    }

    #[test]
    fn test_batch_with_producer_thread() {
        let mut batch =
            BufBatchChannel::with_producer_thread(2, Duration::from_secs(10), 10, |sender| {
                sender.send(Command::Append(1)).unwrap();
                sender.send(Command::Append(2)).unwrap();
                sender.send(Command::Append(3)).unwrap();
                sender.send(Command::Append(4)).unwrap();
            });

        assert_matches!(batch.next(), Ok(drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        ); // max_size

        assert_matches!(batch.next(), Ok(drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [3, 4])
        ); // max_size
    }

    #[test]
    fn test_batch_max_duration() {
        let mut batch =
            BufBatchChannel::with_producer_thread(2, Duration::from_millis(100), 10, |sender| {
                sender.send(Command::Append(1)).unwrap();
                std::thread::sleep(Duration::from_millis(500));
            });

        assert_matches!(batch.next(), Ok(drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1])
        ); // max_duration
        assert!(!batch.is_disconnected()); // check if Flush result was not because thread has finished
    }

    #[test]
    fn test_batch_disconnected() {
        let mut batch =
            BufBatchChannel::with_producer_thread(2, Duration::from_secs(10), 10, |sender| {
                sender.send(Command::Append(1)).unwrap();
            });

        assert_matches!(batch.next(), Ok(drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1])
        ); // disconnected
        assert_matches!(batch.next(), Err(EndOfStreamError));
    }

    #[test]
    fn test_batch_command_complete() {
        let mut batch =
            BufBatchChannel::with_producer_thread(2, Duration::from_secs(10), 10, |sender| {
                sender.send(Command::Append(1)).unwrap();
                sender.send(Command::Flush).unwrap();
                sender.send(Command::Append(2)).unwrap();
                sender.send(Command::Flush).unwrap();
            });

        assert_matches!(batch.next(), Ok(drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1])
        ); // command

        assert_matches!(batch.next(), Ok(drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [2])
        ); // command
    }

    #[test]
    fn test_drain_to_end() {
        let (sender, batch) = BufBatchChannel::new(4, Duration::from_secs(10), 10);

        sender.send(Command::Append(1)).unwrap();
        sender.send(Command::Flush).unwrap();
        sender.send(Command::Append(2)).unwrap();
        sender.send(Command::Flush).unwrap();
        sender.send(Command::Append(3)).unwrap();
        sender.send(Command::Append(4)).unwrap();
        sender.send(Command::Append(5)).unwrap();
        drop(sender);

        assert_eq!(batch.drain_to_end().collect::<Vec<_>>().as_slice(), [1, 2, 3, 4, 5]);
    }
}

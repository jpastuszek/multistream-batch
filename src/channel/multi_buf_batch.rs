/*!
This module provides `MultiBufBatchChannel` that will buffer items into multiple internal batches based on batch stream key until
one of the batches is ready and provide them in one go, along with the batch stream key, using `Drain` iterator.

# Example

Collect batches of items from two streams by reaching different individual batch limits and using `Flush` command.

```rust
use multistream_batch::channel::multi_buf_batch::MultiBufBatchChannel;
use multistream_batch::channel::multi_buf_batch::Command::*;
use std::time::Duration;
use assert_matches::assert_matches;

// Create producer thread with `MultiBufBatchChannel` configured with a maximum size of 4 items and
// a maximum batch duration since the first received item of 200 ms.
let mut batch = MultiBufBatchChannel::with_producer_thread(4, Duration::from_millis(200), 10, |sender| {
    // Send a sequence of `Append` commands with integer stream key and item value
    sender.send(Append(1, 1)).unwrap();
    sender.send(Append(0, 1)).unwrap();
    sender.send(Append(1, 2)).unwrap();
    sender.send(Append(0, 2)).unwrap();
    sender.send(Append(1, 3)).unwrap();
    sender.send(Append(0, 3)).unwrap();
    sender.send(Append(1, 4)).unwrap();
    // At this point batch with stream key `1` should have reached its capacity of 4 items
    sender.send(Append(0, 4)).unwrap();
    // At this point batch with stream key `0` should have reached its capacity of 4 items

    // Send some more to buffer up for next batch
    sender.send(Append(0, 5)).unwrap();
    sender.send(Append(1, 5)).unwrap();
    sender.send(Append(1, 6)).unwrap();
    sender.send(Append(0, 6)).unwrap();

    // Introduce delay to trigger maximum duration timeout
    std::thread::sleep(Duration::from_millis(400));

    // Send items that will be flushed by `Flush` command
    sender.send(Append(0, 7)).unwrap();
    sender.send(Append(1, 7)).unwrap();
    sender.send(Append(1, 8)).unwrap();
    sender.send(Append(0, 8)).unwrap();
    // Flush outstanding items for batch with stream key `1` and `0`
    sender.send(Flush(1)).unwrap();
    sender.send(Flush(0)).unwrap();

    // Last buffered up items will be flushed automatically when this thread exits
    sender.send(Append(0, 9)).unwrap();
    sender.send(Append(1, 9)).unwrap();
    sender.send(Append(1, 10)).unwrap();
    sender.send(Append(0, 10)).unwrap();
    // Exiting closure will shutdown the producer thread
});

// Batches flushed due to individual batch size limit
assert_matches!(batch.next(), Ok((1, drain)) =>
    assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
);

assert_matches!(batch.next(), Ok((0, drain)) =>
    assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
);

// Batches flushed due to duration limit
assert_matches!(batch.next(), Ok((0, drain)) =>
    assert_eq!(drain.collect::<Vec<_>>().as_slice(), [5, 6])
);

assert_matches!(batch.next(), Ok((1, drain)) =>
    assert_eq!(drain.collect::<Vec<_>>().as_slice(), [5, 6])
);

// Batches flushed by sending `Flush` command starting from batch with stream key `1`
assert_matches!(batch.next(), Ok((1, drain)) =>
    assert_eq!(drain.collect::<Vec<_>>().as_slice(), [7, 8])
);

assert_matches!(batch.next(), Ok((0, drain)) =>
    assert_eq!(drain.collect::<Vec<_>>().as_slice(), [7, 8])
);

// Batches flushed by dropping sender (thread exit)
assert_matches!(batch.next(), Ok((0, drain)) =>
    assert_eq!(drain.collect::<Vec<_>>().as_slice(), [9, 10])
);

assert_matches!(batch.next(), Ok((1, drain)) =>
    assert_eq!(drain.collect::<Vec<_>>().as_slice(), [9, 10])
);
```
!*/

use crate::channel::EndOfStreamError;
pub use crate::multi_buf_batch::Stats;
use crate::multi_buf_batch::{MultiBufBatch, PollResult};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};

use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;
use std::vec::Drain;

/// Commands that can be sent to `MultiBufBatchChannel` via `CommandSender` endpoint.
#[derive(Debug)]
pub enum Command<K: Debug + Ord + Hash, I: Debug> {
    /// Append item `I` to batch with stream key `K`.
    Append(K, I),
    /// Flush outstanding items from batch with stream key `K`.
    Flush(K),
}

pub type CommandSender<K, I> = Sender<Command<K, I>>;

/// Collects items into multiple batches based on the stream key.
/// A batch may become ready after collecting `max_size` number of items or until `max_duration` has elapsed
/// since the first item appended to the batch.
///
/// Batch item buffers are cached and reused to avoid allocations.
#[derive(Debug)]
pub struct MultiBufBatchChannel<K: Debug + Ord + Hash, I: Debug> {
    channel: Receiver<Command<K, I>>,
    batch: MultiBufBatch<K, I>,
    // When flushing outstanding batches this iterator will yield keys of outstanding batches to be flushed in order
    flush: Option<std::vec::IntoIter<K>>,
}

impl<K, I> MultiBufBatchChannel<K, I>
where
    K: Debug + Ord + Hash + Send + Clone + 'static,
    I: Debug + Send + 'static,
{
    /// Crates new instance with given maximum batch size (`max_size`) and maximum duration (`max_duration`) that
    /// batch can last since the first item appended to it.
    /// Parameter `channel_size` defines the maximum number of messages that can be buffered between sender and receiver.
    ///
    /// This method also returns `CommandSender` endpoint that can be used to send `Command`s.
    ///
    /// Panics if `max_size == 0`.
    pub fn new(
        max_size: usize,
        max_duration: Duration,
        channel_size: usize,
    ) -> (CommandSender<K, I>, MultiBufBatchChannel<K, I>) {
        let (sender, receiver) = crossbeam_channel::bounded(channel_size);

        (
            sender,
            MultiBufBatchChannel {
                channel: receiver,
                batch: MultiBufBatch::new(max_size, max_duration),
                flush: None,
            },
        )
    }

    /// Calls `producer` closure with `CommandSender` end of the channel in a newly started thread and
    /// returns `MultiBufBatchChannel` connected to that `CommandSender`.
    pub fn with_producer_thread(
        max_size: usize,
        max_duration: Duration,
        channel_size: usize,
        producer: impl FnOnce(CommandSender<K, I>) -> () + Send + 'static,
    ) -> MultiBufBatchChannel<K, I> {
        let (sender, batch) = MultiBufBatchChannel::new(max_size, max_duration, channel_size);

        std::thread::Builder::new().name("MultiBufBatchChannel producer".to_string()).spawn(move || producer(sender)).expect("failed to start producer thread");

        batch
    }

    // Note that some construct here would require polonius support in rustc to be
    // optimal: https://gist.github.com/jpastuszek/559bc637c2715248bac62822a710ad36

    /// Gets next ready batch as pair of batch stream key `K` and `Drain` iterator of its items.
    ///
    /// This call will block until one of the batches becomes ready.
    ///
    /// When the `CommandSender` end has dropped, this method returns with `Err(EndOfStreamError)` after all
    /// outstanding items were flushed.
    pub fn next<'i>(&'i mut self) -> Result<(K, Drain<I>), EndOfStreamError> {
        loop {
            if self.flush.is_some() {
                // TODO: can't do &mut call here whithout polonius
                let keys = self.flush.as_mut().unwrap();

                if let Some(key) = keys.next() {
                    let batch = self.drain(&key).expect("flushing key that does not exist");
                    return Ok((key, batch));
                }
                return Err(EndOfStreamError);
            }

            // Check if we have a ready batch due to any limit or go fetch next item
            let ready_after = match self.batch.poll() {
                PollResult::Ready(key) => {
                    let batch = self.batch.drain(&key).expect("ready key not found");
                    return Ok((key, batch));
                }
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
                Ok(Command::Flush(key)) => {
                    // Mark as complete by producer
                    if self.batch.get(&key).is_some() {
                        // TODO: can't do &mut call here without polonius
                        let batch = self.batch.drain(&key).unwrap();
                        return Ok((key, batch));
                    }
                    continue;
                }
                Ok(Command::Append(key, item)) => {
                    self.batch.append(key, item);
                    continue;
                }
                Err(_eos) => {
                    // Flush batches and free memory
                    let keys: Vec<K> = self.batch.outstanding().cloned().collect();
                    self.batch.clear_cache();
                    self.flush = Some(keys.into_iter());
                    continue;
                }
            }
        }
    }

    /// Lists keys of outstanding batches.
    pub fn outstanding(&self) -> impl Iterator<Item = &K> {
        self.batch.outstanding()
    }

    /// Starts new batch dropping all buffered items.
    pub fn clear(&mut self, key: &K) {
        self.batch.clear(key)
    }

    /// Starts new batch by draining all buffered items.
    pub fn drain(&mut self, key: &K) -> Option<Drain<I>> {
        self.batch.drain(key)
    }

    /// Flushes all outstanding batches starting from oldest.
    pub fn flush(&mut self) -> Vec<(K, Vec<I>)> {
        self.batch.flush()
    }

    /// Returns slice of internal item buffer of given outstanding batch.
    pub fn get(&self, key: &K) -> Option<&[I]> {
        self.batch.get(key)
    }

    /// Drops cached batch buffers.
    pub fn clear_cache(&mut self) {
        self.batch.clear_cache()
    }

    /// Provides usage statistics.
    pub fn stats(&self) -> Stats {
        self.batch.stats()
    }

    /// Splits into `MultiBufBatch` item buffer and channel `Receiver` end.
    pub fn split(self) -> (MultiBufBatch<K, I>, Receiver<Command<K, I>>) {
        (self.batch, self.channel)
    }
}

#[cfg(test)]
mod tests {
    use super::Command::*;
    pub use super::*;
    use assert_matches::assert_matches;
    use std::time::Duration;

    #[test]
    fn test_batch_max_size() {
        let (sender, mut batch) = MultiBufBatchChannel::new(4, Duration::from_secs(10), 20);

        sender.send(Append(0, 1)).unwrap();
        sender.send(Append(0, 2)).unwrap();
        sender.send(Append(0, 3)).unwrap();
        sender.send(Append(0, 4)).unwrap();
        sender.send(Append(0, 5)).unwrap();

        assert_matches!(batch.next(), Ok((0, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        sender.send(Append(1, 1)).unwrap();
        sender.send(Append(1, 2)).unwrap();
        sender.send(Append(1, 3)).unwrap();
        sender.send(Append(1, 4)).unwrap();
        sender.send(Append(1, 5)).unwrap();

        assert_matches!(batch.next(), Ok((1, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        sender.send(Append(1, 6)).unwrap();
        sender.send(Append(0, 6)).unwrap();
        sender.send(Append(1, 7)).unwrap();
        sender.send(Append(0, 7)).unwrap();
        sender.send(Append(1, 8)).unwrap();
        sender.send(Append(0, 8)).unwrap();

        assert_matches!(batch.next(), Ok((1, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [5, 6, 7, 8])
        );

        assert_matches!(batch.next(), Ok((0, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [5, 6, 7, 8])
        );
    }

    #[test]
    fn test_batch_with_producer_thread() {
        let mut batch =
            MultiBufBatchChannel::with_producer_thread(2, Duration::from_secs(10), 20, |sender| {
                sender.send(Append(0, 1)).unwrap();
                sender.send(Append(1, 1)).unwrap();
                sender.send(Append(0, 2)).unwrap();
                sender.send(Append(1, 2)).unwrap();
            });

        assert_matches!(batch.next(), Ok((0, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        );

        assert_matches!(batch.next(), Ok((1, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        );
    }

    #[test]
    fn test_batch_max_duration() {
        let mut batch = MultiBufBatchChannel::with_producer_thread(
            2,
            Duration::from_millis(100),
            10,
            |sender| {
                sender.send(Append(0, 1)).unwrap();
                std::thread::sleep(Duration::from_millis(500));
                sender.send(Append(0, 2)).unwrap();
            },
        );

        assert_matches!(batch.next(), Ok((0, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1])
        );

        assert_matches!(batch.next(), Ok((0, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [2])
        );
    }

    #[test]
    fn test_batch_disconnected() {
        let (sender, mut batch) = MultiBufBatchChannel::new(2, Duration::from_secs(10), 20);

        sender.send(Append(0, 1)).unwrap();
        sender.send(Append(1, 1)).unwrap();
        sender.send(Append(0, 2)).unwrap();
        sender.send(Append(1, 2)).unwrap();
        sender.send(Append(0, 3)).unwrap();
        sender.send(Append(1, 3)).unwrap();

        drop(sender);

        assert_matches!(batch.next(), Ok((0, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        );

        assert_matches!(batch.next(), Ok((1, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        );

        assert_matches!(batch.next(), Ok((0, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [3])
        );

        assert_matches!(batch.next(), Ok((1, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [3])
        );

        assert_matches!(batch.next(), Err(EndOfStreamError));
    }

    #[test]
    fn test_batch_drain() {
        let (sender, mut batch) = MultiBufBatchChannel::new(2, Duration::from_secs(10), 20);

        sender.send(Append(0, 1)).unwrap();
        sender.send(Append(1, 1)).unwrap();
        sender.send(Flush(0)).unwrap();
        sender.send(Append(0, 2)).unwrap();
        sender.send(Append(1, 2)).unwrap();
        sender.send(Append(0, 3)).unwrap();
        sender.send(Append(1, 3)).unwrap();
        sender.send(Flush(1)).unwrap();

        assert_matches!(batch.next(), Ok((0, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1])
        );

        assert_matches!(batch.next(), Ok((1, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        );

        assert_matches!(batch.next(), Ok((0, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [2, 3])
        );

        assert_matches!(batch.next(), Ok((1, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [3])
        );
    }
}

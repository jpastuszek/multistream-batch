use crossbeam_channel::{Sender, Receiver, RecvTimeoutError};
use crate::EndOfStreamError;
use super::{MultistreamBatch, PollResult};

use std::hash::Hash;
use std::fmt::Debug;
use std::time::Duration;
use std::vec::Drain;

/// Commands that can be send to `MultistreamBatchChannel` via `Sender` endpoint.
#[derive(Debug)]
pub enum Command<K: Debug + Ord + Hash, I: Debug> {
    /// Append item `I` to batch for stream with key `K`.
    Append(K, I),
    /// Drain outstanding items from stream `K`.
    Drain(K),
}

/// Result of batching operation
#[derive(Debug)]
pub enum BatchResult<'i, K, I> {
    /// Batch is now complete.
    Complete(K, Drain<'i, I>),
    /// Work has been done but no bach is ready yet.
    TryAgain,
}

/// Collect items into multiple batches based on stream key.
/// This implementation uses `crossbeam` channel to implement timeouts on batch duration limit.
/// 
/// Batches have maximum size and maximum duration (since first item received) limits set and when reached that batch will be flushed.
/// Batches can also be manually flushed by sending `Command::Drain(K)` message.
/// 
#[derive(Debug)]
pub struct MultistreamBatchChannel<K: Debug + Ord + Hash, I: Debug> {
    channel: Receiver<Command<K, I>>,
    batch: MultistreamBatch<K, I>,
    // When flushing outstanding batches this buffer will contain all the data
    flush: Option<Vec<(K, Vec<I>)>>,
    flush_index: usize,
    // Instant at which a batch would reach its duration limit
}

impl<K, I> MultistreamBatchChannel<K, I> where K: Debug + Ord + Hash + Send + Clone + 'static, I: Debug + Send + 'static {
    /// Creates new instance of `MultistreamBatchChannel` given maximum size of any single batch in number of items (`max_size`)
    /// and maximum duration a batch can last (`max_duration`). 
    /// 
    /// Internally bounded `crossbeam` channel is used with `channel_size` capacity.
    /// 
    /// Returns `Sender<Command<K, I>>` side of channel which can be used to send items and flush buffers via `Command` enum 
    /// and `MultistreamBatchChannel` receiver end which can be used to receive flushed batches.
    pub fn new(max_size: usize, max_duration: Duration, channel_size: usize) -> (Sender<Command<K, I>>, MultistreamBatchChannel<K, I>) {
        let (sender, receiver) = crossbeam_channel::bounded(channel_size);

        (sender, MultistreamBatchChannel {
            channel: receiver,
            batch: MultistreamBatch::new(max_size, max_duration),
            flush: None,
            flush_index: 0,
        })
    }

    /// Creates `MultistreamBatchChannel` with sender end embedded in newly spawned thread.
    /// 
    /// `producer` closure will be called in context of newly spawned thread with `Sender<Command<K, I>>` endpoint provided as first argument.
    /// Returns `MultistreamBatchChannel` connected with the sender.
    pub fn with_producer_thread(max_size: usize, max_duration: Duration, channel_size: usize, producer: impl Fn(Sender<Command<K, I>>) -> () + Send + 'static) -> MultistreamBatchChannel<K, I> {
        let (sender, batch) = MultistreamBatchChannel::new(max_size, max_duration, channel_size);

        std::thread::spawn(move || {
            producer(sender)
        });

        batch
    }

    /* Won't compile without polonius support in rustc: https://gist.github.com/jpastuszek/559bc637c2715248bac62822a710ad36
    pub fn next_batch<'i>(&'i mut self) -> Result<(K, Drain<'i, I>), EndOfStreamError> {
        loop {
            match self.next() {
                Ok(None) => continue,
                Ok(Some(kdrain)) => return Ok(kdrain),
                Err(err) => return Err(err),
            }
        }
    }
    */

    /// Gets next batch as pair of key and drain iterator for items.
    ///
    /// This call may block until item is received or batch duration limit was reached.
    /// It may also return `Ok(BatchResult::TryAgain)` if no batch was ready at this time without blocking. 
    /// In this case client should call `next()` again.
    ///
    /// Returns `Err(EndOfStreamError)` after `Sender` end was dropped and all outstanding batches were flushed.
    pub fn next<'i>(&'i mut self) -> Result<BatchResult<'i, K, I>, EndOfStreamError> {
        if self.flush.is_some() {
            // Note that I can't use `if let` above or we move mut borrow here
            let batches = self.flush.as_mut().unwrap();

            if self.flush_index >= batches.len() {
                // We are done flushing, free memory and bail
                batches.clear();
                return Err(EndOfStreamError);
            }

            // Provide next batch from flushed batches
            let (key, items) = &mut batches[self.flush_index];
            self.flush_index += 1;
            return Ok(BatchResult::Complete(key.clone(), items.drain(0..)))
        }

        // Check if we have a ready batch due to any limit or go fetch next item
        let ready_after = match self.batch.poll() {
            PollResult::Ready(key) => {
                let drain = self.batch.drain(&key).expect("key ready but drain did not find it");
                return Ok(BatchResult::Complete(key, drain))
            }
            PollResult::NotReady(ready_after) => ready_after,
        };

        let recv_result = if let Some(ready_after) = ready_after {
            match self.channel.recv_timeout(ready_after) {
                // We got new item before timeout was reached
                Ok(item) => Ok(item),
                // A batch should be ready now; try again
                Err(RecvTimeoutError::Timeout) => return Ok(BatchResult::TryAgain),
                // Other end gone
                Err(RecvTimeoutError::Disconnected) => Err(EndOfStreamError),
            }
        } else {
            // No outstanding batches; wait for first item
            self.channel.recv().map_err(|_| EndOfStreamError)
        };

        match recv_result {
            Ok(Command::Drain(key)) => {
                // Mark as complete by producer
                if let Some(drain) = self.batch.drain(&key) {
                    return Ok(BatchResult::Complete(key, drain))
                }
            },
            Ok(Command::Append(key, item)) => self.batch.append(key, item),
            Err(_eos) => {
                // Flush batches and free memory
                let batches = self.batch.flush();
                self.batch.clear_cache();
                self.flush = Some(batches);
            }
        }

        return Ok(BatchResult::TryAgain)
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    use std::time::Duration;
    use assert_matches::assert_matches;
    use super::Command::*;

    #[test]
    fn test_batch_max_size() {
        let (sender, mut batch) = MultistreamBatchChannel::new(4, Duration::from_secs(10), 20);

        sender.send(Append(0, 1)).unwrap();
        sender.send(Append(0, 2)).unwrap();
        sender.send(Append(0, 3)).unwrap();
        sender.send(Append(0, 4)).unwrap();
        sender.send(Append(0, 5)).unwrap();

        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));
        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));
        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));
        assert_matches!(batch.next(), Ok(BatchResult::TryAgain)); // max_size

        assert_matches!(batch.next(), Ok(BatchResult::Complete(0, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        sender.send(Append(1, 1)).unwrap();
        sender.send(Append(1, 2)).unwrap();
        sender.send(Append(1, 3)).unwrap();
        sender.send(Append(1, 4)).unwrap();
        sender.send(Append(1, 5)).unwrap();

        assert_matches!(batch.next(), Ok(BatchResult::TryAgain)); // 0, 5

        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));
        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));
        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));
        assert_matches!(batch.next(), Ok(BatchResult::TryAgain)); // max_size

        assert_matches!(batch.next(), Ok(BatchResult::Complete(1, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        sender.send(Append(1, 6)).unwrap();
        sender.send(Append(0, 6)).unwrap();
        sender.send(Append(1, 7)).unwrap();
        sender.send(Append(0, 7)).unwrap();
        sender.send(Append(1, 8)).unwrap();
        sender.send(Append(0, 8)).unwrap();

        assert_matches!(batch.next(), Ok(BatchResult::TryAgain)); // 1, 5

        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));
        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));
        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));
        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));
        assert_matches!(batch.next(), Ok(BatchResult::TryAgain)); // max_size

        assert_matches!(batch.next(), Ok(BatchResult::Complete(1, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [5, 6, 7, 8])
        );

        assert_matches!(batch.next(), Ok(BatchResult::TryAgain)); // max_size

        assert_matches!(batch.next(), Ok(BatchResult::Complete(0, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [5, 6, 7, 8])
        );
    }

    #[test]
    fn test_batch_with_producer_thread() {
        let mut batch = MultistreamBatchChannel::with_producer_thread(2, Duration::from_secs(10), 20, |sender| {
            sender.send(Append(0, 1)).unwrap();
            sender.send(Append(1, 1)).unwrap();
            sender.send(Append(0, 2)).unwrap();
            sender.send(Append(1, 2)).unwrap();
        });


        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));
        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));
        assert_matches!(batch.next(), Ok(BatchResult::TryAgain)); // max_size

        assert_matches!(batch.next(), Ok(BatchResult::Complete(0, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        );

        assert_matches!(batch.next(), Ok(BatchResult::TryAgain)); // max_size

        assert_matches!(batch.next(), Ok(BatchResult::Complete(1, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        );
    }

    #[test]
    fn test_batch_max_duration() {
        let mut batch = MultistreamBatchChannel::with_producer_thread(2, Duration::from_millis(100) ,10, |sender| {
            sender.send(Append(0, 1)).unwrap();
            std::thread::sleep(Duration::from_millis(500));
        });

        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));
        assert_matches!(batch.next(), Ok(BatchResult::TryAgain)); // timeout

        assert_matches!(batch.next(), Ok(BatchResult::Complete(0, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1])
        );
    }

    #[test]
    fn test_batch_disconnected() {
        let (sender, mut batch) = MultistreamBatchChannel::new(2, Duration::from_secs(10), 20);

        sender.send(Append(0, 1)).unwrap();
        sender.send(Append(1, 1)).unwrap();
        sender.send(Append(0, 2)).unwrap();
        sender.send(Append(1, 2)).unwrap();
        sender.send(Append(0, 3)).unwrap();
        sender.send(Append(1, 3)).unwrap();

        drop(sender);

        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));
        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));
        assert_matches!(batch.next(), Ok(BatchResult::TryAgain)); // max_size

        assert_matches!(batch.next(), Ok(BatchResult::Complete(0, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        );

        assert_matches!(batch.next(), Ok(BatchResult::TryAgain)); // max_size

        assert_matches!(batch.next(), Ok(BatchResult::Complete(1, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        );

        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));
        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));

        assert_matches!(batch.next(), Ok(BatchResult::TryAgain)); // EndOfStreamError will flush

        assert_matches!(batch.next(), Ok(BatchResult::Complete(0, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [3])
        );

        assert_matches!(batch.next(), Ok(BatchResult::Complete(1, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [3])
        );

        assert_matches!(batch.next(), Err(EndOfStreamError));
    }

    #[test]
    fn test_batch_drain() {
        let (sender, mut batch) = MultistreamBatchChannel::new(2, Duration::from_secs(10), 20);

        sender.send(Append(0, 1)).unwrap();
        sender.send(Append(1, 1)).unwrap();
        sender.send(Drain(0)).unwrap();
        sender.send(Append(0, 2)).unwrap();
        sender.send(Append(1, 2)).unwrap();
        sender.send(Append(0, 3)).unwrap();
        sender.send(Append(1, 3)).unwrap();
        sender.send(Drain(1)).unwrap();

        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));
        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));

        assert_matches!(batch.next(), Ok(BatchResult::Complete(0, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1])
        );

        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));
        assert_matches!(batch.next(), Ok(BatchResult::TryAgain)); // max_size

        assert_matches!(batch.next(), Ok(BatchResult::Complete(1, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        );

        assert_matches!(batch.next(), Ok(BatchResult::TryAgain)); // max_size

        assert_matches!(batch.next(), Ok(BatchResult::Complete(0, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [2, 3])
        );

        assert_matches!(batch.next(), Ok(BatchResult::TryAgain));

        assert_matches!(batch.next(), Ok(BatchResult::Complete(1, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [3])
        );
    }
}

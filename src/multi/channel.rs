use crossbeam_channel::{Sender, Receiver, RecvTimeoutError};
use crate::EndOfStreamError;
use super::{MultistreamBatch, PollResult};

use std::hash::Hash;
use std::fmt::Debug;
use std::time::{Duration, Instant};
use std::vec::Drain;

/// Commands that can be send to `MultistreamBatchChannel` via `Sender` endpoint
#[derive(Debug)]
pub enum Command<K: Debug + Ord + Hash, T: Debug> {
    /// Append item `T` to batch for stream with key `K`
    Append(K, T),
    /// Drain outstanding items from stream `K`
    Drain(K),
}

#[derive(Debug)]
pub struct MultistreamBatchChannel<K: Debug + Ord + Hash, T: Debug> {
    channel: Receiver<Command<K, T>>,
    mbatch: MultistreamBatch<K, T>,
    flush: Option<Vec<(K, Vec<T>)>>,
    flush_index: usize,
    // Instant at after which we can poll batch
    next_batch_at: Option<Instant>,
}

impl<K, T> MultistreamBatchChannel<K, T> where K: Debug + Ord + Hash + Send + Clone + 'static, T: Debug + Send + 'static {
    pub fn new(max_size: usize, max_duration: Duration, channel_size: usize) -> (Sender<Command<K, T>>, MultistreamBatchChannel<K, T>) {
        let (sender, receiver) = crossbeam_channel::bounded(channel_size);

        (sender, MultistreamBatchChannel {
            channel: receiver,
            mbatch: MultistreamBatch::new(max_size, max_duration),
            flush: None,
            flush_index: 0,
            next_batch_at: None,
        })
    }

    pub fn with_producer_thread(max_size: usize, max_duration: Duration, channel_size: usize, producer: impl Fn(Sender<Command<K, T>>) -> () + Send + 'static) -> MultistreamBatchChannel<K, T> {
        let (sender, batch) = MultistreamBatchChannel::new(max_size, max_duration, channel_size);

        std::thread::spawn(move || {
            producer(sender)
        });

        batch
    }

    /* Won't compile without polonius support in rustc: https://gist.github.com/jpastuszek/559bc637c2715248bac62822a710ad36
    pub fn next_batch<'i>(&'i mut self) -> Result<(K, Drain<'i, T>), EndOfStreamError> {
        loop {
            match self.next() {
                Ok(None) => continue,
                Ok(Some(kdrain)) => return Ok(kdrain),
                Err(err) => return Err(err),
            }
        }
    }
    */

    /// Get next batch as pair of key and drain iterator for items.
    ///
    /// Call may block until item is received or batch duration limit was reached.
    ///
    /// Retruns `Ok(None)` if no batch was ready at this call, call again.
    /// Returns `Err(EndOfStreamError)` after `Sender` end was dropped and all outstanding batches
    /// were flushed.
    pub fn next<'i>(&'i mut self) -> Result<Option<(K, Drain<'i, T>)>, EndOfStreamError> {
        if self.flush.is_some() {
            // Note that I can't use if let above or we move mut borrow here
            let batches = self.flush.as_mut().unwrap();

            if self.flush_index >= batches.len() {
                // We are done flushing, free memory and bail
                batches.clear();
                return Err(EndOfStreamError);
            }

            let (key, items) = &mut batches[self.flush_index];
            self.flush_index += 1;
            return Ok(Some((key.clone(), items.drain(0..))))
        }

        let now = Instant::now();

        // Check if batch is ready due to duration limit
        let recv_result = match self.next_batch_at.map(|instant| if instant <= now { None } else { Some(instant) }) {
            // Batch ready to go
            Some(None) => {
                // We should have ready batch but if not update next_batch_at and go again
                match self.mbatch.poll() {
                    PollResult::Ready(key, drain) => return Ok(Some((key, drain))),
                    PollResult::NotReady(instant) => {
                        // Update instant here as batch could have been already returned by append
                        self.next_batch_at = instant;
                        return Ok(None)
                    }
                }
            }
            // Wait for next batch or item
            Some(Some(instant)) => match self.channel.recv_timeout(instant.duration_since(now)) {
                Ok(item) => Ok(item),
                // A batch should be ready now; go again
                Err(RecvTimeoutError::Timeout) => return Ok(None),
                // Other end gone
                Err(RecvTimeoutError::Disconnected) => Err(EndOfStreamError),
            },
            // No outstanding batches so wait for first item
            None => self.channel.recv().map_err(|_| EndOfStreamError),
        };

        match recv_result {
            Ok(Command::Append(key, item)) => match self.mbatch.append(key, item) {
                PollResult::Ready(key, drain) => return Ok(Some((key, drain))),
                PollResult::NotReady(instant) => {
                    self.next_batch_at = instant;
                    return Ok(None)
                }
            },
            Ok(Command::Drain(key)) => return Ok(self.mbatch.drain_stream(key)),
            Err(_eos) => {
                // Flush batches and free memory
                let batches = self.mbatch.flush();
                self.mbatch.clear_cache();
                self.flush = Some(batches);

                // There won't be next batch
                self.next_batch_at.take();

                return Ok(None)
            }
        }
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
        let (sender, mut mbatch) = MultistreamBatchChannel::new(4, Duration::from_secs(10), 20);

        sender.send(Append(0, 1)).unwrap();
        sender.send(Append(0, 2)).unwrap();
        sender.send(Append(0, 3)).unwrap();
        sender.send(Append(0, 4)).unwrap();
        sender.send(Append(0, 5)).unwrap();

        assert_matches!(mbatch.next(), Ok(None));
        assert_matches!(mbatch.next(), Ok(None));
        assert_matches!(mbatch.next(), Ok(None));

        assert_matches!(mbatch.next(), Ok(Some((0, drain))) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        sender.send(Append(1, 1)).unwrap();
        sender.send(Append(1, 2)).unwrap();
        sender.send(Append(1, 3)).unwrap();
        sender.send(Append(1, 4)).unwrap();
        sender.send(Append(1, 5)).unwrap();

        assert_matches!(mbatch.next(), Ok(None)); // 0, 5

        assert_matches!(mbatch.next(), Ok(None));
        assert_matches!(mbatch.next(), Ok(None));
        assert_matches!(mbatch.next(), Ok(None));

        assert_matches!(mbatch.next(), Ok(Some((1, drain))) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        sender.send(Append(1, 6)).unwrap();
        sender.send(Append(0, 6)).unwrap();
        sender.send(Append(1, 7)).unwrap();
        sender.send(Append(0, 7)).unwrap();
        sender.send(Append(1, 8)).unwrap();
        sender.send(Append(0, 8)).unwrap();

        assert_matches!(mbatch.next(), Ok(None)); // 1, 5

        assert_matches!(mbatch.next(), Ok(None));
        assert_matches!(mbatch.next(), Ok(None));
        assert_matches!(mbatch.next(), Ok(None));
        assert_matches!(mbatch.next(), Ok(None));

        assert_matches!(mbatch.next(), Ok(Some((1, drain))) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [5, 6, 7, 8])
        );

        assert_matches!(mbatch.next(), Ok(Some((0, drain))) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [5, 6, 7, 8])
        );
    }

    #[test]
    fn test_batch_with_producer_thread() {
        let mut mbatch = MultistreamBatchChannel::with_producer_thread(2, Duration::from_secs(10), 20, |sender| {
            sender.send(Append(0, 1)).unwrap();
            sender.send(Append(1, 1)).unwrap();
            sender.send(Append(0, 2)).unwrap();
            sender.send(Append(1, 2)).unwrap();
        });


        assert_matches!(mbatch.next(), Ok(None));
        assert_matches!(mbatch.next(), Ok(None));

        assert_matches!(mbatch.next(), Ok(Some((0, drain))) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        );

        assert_matches!(mbatch.next(), Ok(Some((1, drain))) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        );
    }

    #[test]
    fn test_batch_max_duration() {
        let mut mbatch = MultistreamBatchChannel::with_producer_thread(2, Duration::from_millis(100) ,10, |sender| {
            sender.send(Append(0, 1)).unwrap();
            std::thread::sleep(Duration::from_millis(500));
        });

        assert_matches!(mbatch.next(), Ok(None));
        assert_matches!(mbatch.next(), Ok(None)); // timeout

        assert_matches!(mbatch.next(), Ok(Some((0, drain))) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1])
        );
    }

    #[test]
    fn test_batch_disconnected() {
        let (sender, mut mbatch) = MultistreamBatchChannel::new(2, Duration::from_secs(10), 20);

        sender.send(Append(0, 1)).unwrap();
        sender.send(Append(1, 1)).unwrap();
        sender.send(Append(0, 2)).unwrap();
        sender.send(Append(1, 2)).unwrap();
        sender.send(Append(0, 3)).unwrap();
        sender.send(Append(1, 3)).unwrap();

        drop(sender);

        assert_matches!(mbatch.next(), Ok(None));
        assert_matches!(mbatch.next(), Ok(None));

        assert_matches!(mbatch.next(), Ok(Some((0, drain))) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        );

        assert_matches!(mbatch.next(), Ok(Some((1, drain))) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        );

        assert_matches!(mbatch.next(), Ok(None));
        assert_matches!(mbatch.next(), Ok(None));

        assert_matches!(mbatch.next(), Ok(None)); // EndOfStreamError will flush

        assert_matches!(mbatch.next(), Ok(Some((0, drain))) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [3])
        );

        assert_matches!(mbatch.next(), Ok(Some((1, drain))) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [3])
        );

        assert_matches!(mbatch.next(), Err(EndOfStreamError));
    }

    #[test]
    fn test_batch_drain() {
        let (sender, mut mbatch) = MultistreamBatchChannel::new(2, Duration::from_secs(10), 20);

        sender.send(Append(0, 1)).unwrap();
        sender.send(Append(1, 1)).unwrap();
        sender.send(Drain(0)).unwrap();
        sender.send(Append(0, 2)).unwrap();
        sender.send(Append(1, 2)).unwrap();
        sender.send(Append(0, 3)).unwrap();
        sender.send(Append(1, 3)).unwrap();
        sender.send(Drain(1)).unwrap();

        assert_matches!(mbatch.next(), Ok(None));
        assert_matches!(mbatch.next(), Ok(None));

        assert_matches!(mbatch.next(), Ok(Some((0, drain))) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1])
        );

        assert_matches!(mbatch.next(), Ok(None));

        assert_matches!(mbatch.next(), Ok(Some((1, drain))) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        );

        assert_matches!(mbatch.next(), Ok(Some((0, drain))) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [2, 3])
        );

        assert_matches!(mbatch.next(), Ok(None));

        assert_matches!(mbatch.next(), Ok(Some((1, drain))) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [3])
        );
    }
}

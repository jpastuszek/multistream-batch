use std::fmt::Debug;
use std::time::{Duration, Instant};
use linked_hash_map::LinkedHashMap;
use linked_hash_set::LinkedHashSet;
use std::hash::Hash;
use std::vec::Drain;

mod channel;
pub use channel::*;

/// Represents outstanding batch with items buffer from cache and `Instant` at which it was crated.
#[derive(Debug)]
struct OutstandingBatch<I: Debug> {
    items: Vec<I>,
    created: Instant,
}

impl<I: Debug> OutstandingBatch<I> {
    fn new(capacity: usize) -> OutstandingBatch<I> {
        OutstandingBatch {
            items: Vec::with_capacity(capacity),
            created: Instant::now(),
        }
    }

    fn from_cache(mut items: Vec<I>) -> OutstandingBatch<I> {
        // Make sure nothing is left after undrained
        items.clear();

        OutstandingBatch {
            items,
            created: Instant::now(),
        }
    }
}

/// Represents result from `poll` and `append` functions where batch is `Ready` to be consumed or `NotReady` yet.
#[derive(Debug)]
pub enum PollResult<K: Debug> {
    /// Batch is complete after reaching one of the limits; stream key and
    /// `Drain` iterator for the batch items are provided.
    Ready(K),
    /// No outstanding batch reached a limit; provides optional `Duration` until first `max_duration` limit will be reached
    /// if there is an outstanding stream batch.
    NotReady(Option<Duration>),
}

/// Collect items into multiple batches based on stream key. This base implementation does not handle actual waiting on batch duration timeouts.
/// 
/// When given batch limits are reached iterator draining the batch items is provided.
/// Batch given by stream key can also be manually flushed.
///
/// Batch buffers are cached to avoid allocations.
#[derive(Debug)]
pub struct MultistreamBatch<K: Debug + Ord + Hash, I: Debug> {
    max_size: usize,
    max_duration: Duration,
    // Cache of empty batch item buffers
    cache: Vec<Vec<I>>,
    // Batches that have items in them but has not yet reached any limit in order of insertion
    outstanding: LinkedHashMap<K, OutstandingBatch<I>>,
    // Batch with key K is ready to be consumed due to reaching max_size limit
    full: LinkedHashSet<K>,
}

impl<K, I> MultistreamBatch<K, I> where K: Debug + Ord + Hash + Clone, I: Debug {
    /// Crate new `MultistreamBatch` with given maximum size (`max_size`) of batch and maximum duration (`max_duration`) since batch was crated (first item appended) limits.
    /// 
    /// Panics if `max_size` == 0.
    pub fn new(max_size: usize, max_duration: Duration) -> MultistreamBatch<K, I> {
        assert!(max_size > 0, "MultistreamBatch::new bad max_size");

        MultistreamBatch {
            max_size,
            max_duration,
            cache: Default::default(),
            outstanding: Default::default(),
            full: Default::default(),
        }
    }

    /// Drain outstanding batch with given stream key.
    pub fn drain(&mut self, key: &K) -> Option<Drain<I>> {
        // If consuming full key clear it
        self.full.remove(key);

        // Move items from outstanding to cache
        let items = self.outstanding.remove(key)?.items;
        self.cache.push(items);
        let items = self.cache.last_mut().unwrap();

        // Drain items
        let drain = items.drain(0..);
        Some(drain)
    }

    /// Flush all outstanding stream batches starting from oldest.
    pub fn flush(&mut self) -> Vec<(K, Vec<I>)> {
        let cache = &mut self.cache;
        let outstanding = &mut self.outstanding;

        outstanding.entries().map(|entry| {
            let key = entry.key().clone();

            // Move to cache
            let items = entry.remove().items;
            cache.push(items);
            let items = cache.last_mut().unwrap();

            // Move items out preserving capacity
            let items = items.split_off(0);

            (key, items)
        }).collect()
    }

    /// Drop cached batch buffers.
    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }

    /// Poll for outstanding batches that reached duration limit.
    fn poll(&self) -> PollResult<K> {
        // Check oldest full batch first to make sure that following call to append won't fail
        if let Some(key) = self.full.front() {
            return PollResult::Ready(key.clone())
        }

        // Check oldest outstanding batch
        if let Some((key, batch)) = self.outstanding.front() {
            let since_start = Instant::now().duration_since(batch.created);

            if since_start >= self.max_duration {
                return PollResult::Ready(key.clone())
            }

            return PollResult::NotReady(Some(self.max_duration - since_start))
        }

        return PollResult::NotReady(None)
    }

    /// Append next item to batch with given stream key.
    ///
    /// Returns `PollResult::Ready(K, Drain<I>)` where `K` is key of ready batch and `I`
    /// are the items in the batch.
    ///
    /// Batch will be ready to drain if:
    /// * `max_size` of the batch was reached,
    /// * `max_duration` since first element appended elapsed.
    ///
    /// Returns `PollResult::NotReady(Option<Duration>)` when no batch has reached a limit with
    /// optional `Instance` of time at which oldest batch reaches `max_duration` limit.
    pub fn append(&mut self, key: K, item: I) -> Result<(), I> {
        // Look up batch in outstanding or crate one using cached or new items buffer
        if let Some(batch) = self.outstanding.get_mut(&key) {
            // Reached max_size limit
            if batch.items.len() >= self.max_size {
                return Err(item)
            }

            batch.items.push(item);

            // Mark as full
            if batch.items.len() >= self.max_size {
                self.full.insert(key);
            }
        } else {
            let mut batch = if let Some(items) = self.cache.pop() {
                OutstandingBatch::from_cache(items)
            } else {
                OutstandingBatch::new(self.max_size)
            };

            batch.items.push(item);
            self.outstanding.insert(key.clone(), batch);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    use std::time::Duration;
    use assert_matches::assert_matches;

    #[test]
    fn test_batch_poll() {
        let mut batch = MultistreamBatch::new(4, Duration::from_secs(10));

        // empty has no outstanding batches
        assert_matches!(batch.poll(), PollResult::NotReady(None));

        assert!(batch.append(0, 1).is_ok());

        // now we have outstanding
        assert_matches!(batch.poll(), PollResult::NotReady(Some(_instant)));

        assert!(batch.append(0, 2).is_ok());
        assert!(batch.append(0, 3).is_ok());
        assert!(batch.append(0, 4).is_ok());

        assert_matches!(batch.poll(), PollResult::Ready(0) =>
            assert_eq!(batch.drain(&0).unwrap().collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        // no outstanding again
        assert_matches!(batch.poll(), PollResult::NotReady(None));
    }

    #[test]
    fn test_batch_max_size() {
        let mut batch = MultistreamBatch::new(4, Duration::from_secs(10));

        assert!(batch.append(0, 1).is_ok());
        assert!(batch.append(0, 2).is_ok());
        assert!(batch.append(0, 3).is_ok());
        assert!(batch.append(0, 4).is_ok());

        assert_matches!(batch.poll(), PollResult::Ready(0) =>
            assert_eq!(batch.drain(&0).unwrap().collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        assert!(batch.append(0, 5).is_ok());
        assert!(batch.append(0, 6).is_ok());
        assert!(batch.append(0, 7).is_ok());
        assert!(batch.append(0, 8).is_ok());

        assert_matches!(batch.poll(), PollResult::Ready(0) =>
            assert_eq!(batch.drain(&0).unwrap().collect::<Vec<_>>().as_slice(), [5, 6, 7, 8])
        );

        assert!(batch.append(1, 1).is_ok());
        assert!(batch.append(0, 9).is_ok());
        assert!(batch.append(1, 2).is_ok());
        assert!(batch.append(0, 10).is_ok());
        assert!(batch.append(1, 3).is_ok());
        assert!(batch.append(0, 11).is_ok());
        assert!(batch.append(1, 4).is_ok());

        assert_matches!(batch.poll(), PollResult::Ready(1) =>
            assert_eq!(batch.drain(&1).unwrap().collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        assert!(batch.append(0, 12).is_ok());

        assert_matches!(batch.poll(), PollResult::Ready(0) =>
            assert_eq!(batch.drain(&0).unwrap().collect::<Vec<_>>().as_slice(), [9, 10, 11, 12])
        );
    }

/*
    #[test]
    fn test_batch_max_instant() {
        let mut batch = MultistreamBatch::new(4, Duration::from_secs(10));

        assert!(batch.append(0, 1).is_ok());

        let expire_0 = match batch.poll() {
            PollResult::NotReady(Some(instant)) => instant,
            _ => panic!("expected NotReady with instant"),
        };

        assert!(batch.append(0, 2).is_ok());
        assert_matches!(batch.poll(), PollResult::NotReady(Some(instant)) => assert_eq!(instant, expire_0));

        assert!(batch.append(1, 1).is_ok());
        assert_matches!(batch.poll(), PollResult::NotReady(Some(instant)) => assert_eq!(instant, expire_0));

        assert!(batch.append(0, 3).is_ok());
        assert_matches!(batch.poll(), PollResult::NotReady(Some(instant)) => assert_eq!(instant, expire_0));
        assert!(batch.append(1, 2).is_ok());
        assert_matches!(batch.poll(), PollResult::NotReady(Some(instant)) => assert_eq!(instant, expire_0));

        assert!(batch.append(0, 4).is_ok());
        assert_matches!(batch.poll(), PollResult::Ready(0) =>
            assert_eq!(batch.drain(&0).unwrap().collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        assert!(batch.append(1, 3).is_ok());
        assert_matches!(batch.poll(), PollResult::NotReady(Some(instant)) => assert!(instant > expire_0));
    }
*/
    #[test]
    fn test_drain_stream() {
        let mut batch = MultistreamBatch::new(4, Duration::from_secs(10));

        assert!(batch.append(0, 1).is_ok());
        assert!(batch.append(0, 2).is_ok());
        assert!(batch.append(0, 3).is_ok());

        assert!(batch.append(1, 1).is_ok());
        assert!(batch.append(1, 2).is_ok());

        assert_matches!(batch.drain(&1), Some(drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        );

        assert_matches!(batch.drain(&0), Some(drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3])
        );

        assert!(batch.append(0, 5).is_ok());
        assert!(batch.append(0, 6).is_ok());
        assert!(batch.append(0, 7).is_ok());
        assert!(batch.append(0, 8).is_ok());

        assert_matches!(batch.poll(), PollResult::Ready(0) =>
            assert_eq!(batch.drain(&0).unwrap().collect::<Vec<_>>().as_slice(), [5, 6, 7, 8])
        );
    }

    #[test]
    fn test_flush() {
        let mut batch = MultistreamBatch::new(4, Duration::from_secs(10));

        assert!(batch.append(0, 1).is_ok());
        assert!(batch.append(1, 1).is_ok());
        assert!(batch.append(0, 2).is_ok());
        assert!(batch.append(1, 2).is_ok());
        assert!(batch.append(0, 3).is_ok());


        let batches = batch.flush();

        assert_eq!(batches[0].0, 0);
        assert_eq!(batches[0].1.as_slice(), [1, 2, 3]);

        assert_eq!(batches[1].0, 1);
        assert_eq!(batches[1].1.as_slice(), [1, 2]);
    }
}
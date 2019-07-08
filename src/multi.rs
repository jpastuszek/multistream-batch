use std::fmt::Debug;
use std::time::{Duration, Instant};
use linked_hash_map::LinkedHashMap;
use std::hash::Hash;
use std::vec::Drain;

mod channel;
pub use channel::*;

#[derive(Debug)]
struct StreamBatch<I: Debug> {
    items: Vec<I>,
    created: Instant,
}

impl<I: Debug> StreamBatch<I> {
    fn new(capacity: usize) -> StreamBatch<I> {
        StreamBatch {
            items: Vec::with_capacity(capacity),
            created: Instant::now(),
        }
    }

    fn from_cache(mut items: Vec<I>) -> StreamBatch<I> {
        // Make sure nothing is left after undrained
        items.clear();

        StreamBatch {
            items,
            created: Instant::now(),
        }
    }
}

#[derive(Debug)]
pub enum PollResult<'a, K: Debug, I: Debug> {
    /// Batch is complete after reaching one of the limits; stream key and
    /// `Drain` iterator for the batch items are provided.
    Ready(K, Drain<'a, I>),
    /// No outstanding batch reached a limit; provides optional `Instant` until first `max_duration` limit will be reached
    /// if there is an outstanding stream batch.
    NotReady(Option<Instant>),
}

impl<'a, K: Debug, I: Debug> From<(K, Drain<'a, I>)> for PollResult<'a, K, I> {
    fn from(kv: (K, Drain<'a, I>)) -> PollResult<'a, K, I> {
        PollResult::Ready(kv.0, kv.1)
    }
}

/// Collects items into batches based on stream key.
/// When given batch limits are reached iterator draining the batch items is provided.
///
/// Batche buffers are cached to avoid allocations.
#[derive(Debug)]
pub struct MultistreamBatch<K: Debug + Ord + Hash, I: Debug> {
    max_size: usize,
    max_duration: Duration,
    // Cache of empty batch item buffers
    cache: Vec<Vec<I>>,
    // Batches that have items in them but has not yet reached any limit in order of insertion
    outstanding: LinkedHashMap<K, StreamBatch<I>>,
}

impl<K, I> MultistreamBatch<K, I> where K: Debug + Ord + Hash + Clone, I: Debug {
    pub fn new(max_size: usize, max_duration: Duration) -> MultistreamBatch<K, I> {
        // Note that with max_size == 1 the insert function may never get to poll max_duration
        // expired batches
        assert!(max_size > 1, "MultistreamBatch::new bad max_size");

        MultistreamBatch {
            max_size,
            max_duration,
            cache: Default::default(),
            outstanding: Default::default(),
        }
    }

    /// Drain outstanding batch with given stream key.
    pub fn drain_stream(&mut self, key: K) -> Option<(K, Drain<I>)> {
        // Move items from outstanding to cache
        let items = self.outstanding.remove(&key)?.items;
        self.cache.push(items);
        let items = self.cache.last_mut().unwrap();

        // Drain items
        let drain = items.drain(0..);
        Some((key, drain))
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

    /// Pool for outstanding batches that reached duration limit.
    fn poll(&mut self) -> PollResult<K, I> {
        // Check oldest outstanding batch
        if let Some((key, batch)) = self.outstanding.front() {
            let since_start = Instant::now().duration_since(batch.created);
            let key = key.clone();

            // Reached max_duration limit; need to == so that returned instance represents moment
            // in time at which batch is ready
            if since_start >= self.max_duration {
                return self.drain_stream(key).unwrap().into()
            }

            return PollResult::NotReady(Some(batch.created + self.max_duration))
        }

        return PollResult::NotReady(None)
    }

    /// Append next item to a batch with given stream key.
    ///
    /// Returns `PollResult::Ready(K, Drain<I>)` where `K` is key of ready batch and `I`
    /// are the items in the batch.
    ///
    /// Batch will be ready to drain if:
    /// * `max_size` of the batch was reached,
    /// * `max_duration` since first element returned elapsed.
    ///
    /// Returns `PollResult::NotReady(Option<Duration>)` when no batch has reached a limit with
    /// optional `Duration` of time until oldest batch reaches `max_duration` limit.
    pub fn append(&mut self, key: K, item: I) -> PollResult<K, I> {
        // First look up in outstanding or move one from cache/create new batch
        let len = if let Some(batch) = self.outstanding.get_mut(&key) {
            batch.items.push(item);
            batch.items.len()
        } else {
            // Get from cache or allocate new
            let mut batch = if let Some(items) = self.cache.pop() {
                StreamBatch::from_cache(items)
            } else {
                StreamBatch::new(self.max_size)
            };

            // Push item and store in outstanding
            batch.items.push(item);
            self.outstanding.insert(key.clone(), batch);
            1
        };

        // Reached max_size limit
        if len >= self.max_size {
            return self.drain_stream(key).unwrap().into()
        }

        self.poll()
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    use std::time::Duration;
    use assert_matches::assert_matches;

    #[test]
    fn test_batch_poll() {
        let mut mbatch = MultistreamBatch::new(4, Duration::from_secs(10));

        // empty has no outstanding batches
        assert_matches!(mbatch.poll(), PollResult::NotReady(None));

        assert_matches!(mbatch.append(0, 1), PollResult::NotReady(Some(_instant)));

        // now we have outstanding
        assert_matches!(mbatch.poll(), PollResult::NotReady(Some(_instant)));

        assert_matches!(mbatch.append(0, 2), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(0, 3), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(0, 4), PollResult::Ready(0, drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        // no outstanding again
        assert_matches!(mbatch.poll(), PollResult::NotReady(None));
    }

    #[test]
    fn test_batch_max_size() {
        let mut mbatch = MultistreamBatch::new(4, Duration::from_secs(10));

        assert_matches!(mbatch.append(0, 1), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(0, 2), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(0, 3), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(0, 4), PollResult::Ready(0, drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        assert_matches!(mbatch.append(0, 5), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(0, 6), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(0, 7), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(0, 8), PollResult::Ready(0, drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [5, 6, 7, 8])
        );

        assert_matches!(mbatch.append(1, 1), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(0, 9), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(1, 2), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(0, 10), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(1, 3), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(0, 11), PollResult::NotReady(Some(_instant)));

        assert_matches!(mbatch.append(1, 4), PollResult::Ready(1, drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        assert_matches!(mbatch.append(0, 12), PollResult::Ready(0, drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [9, 10, 11, 12])
        );
    }

    #[test]
    fn test_batch_max_instant() {
        let mut mbatch = MultistreamBatch::new(4, Duration::from_secs(10));

        let expire_0 = match mbatch.append(0, 1) {
            PollResult::NotReady(Some(instant)) => instant,
            _ => panic!("expected NotReady with instant"),
        };

        assert_matches!(mbatch.append(0, 2), PollResult::NotReady(Some(instant)) => assert_eq!(instant, expire_0));
        assert_matches!(mbatch.append(1, 1), PollResult::NotReady(Some(instant)) => assert_eq!(instant, expire_0));
        assert_matches!(mbatch.poll(), PollResult::NotReady(Some(instant)) => assert_eq!(instant, expire_0));

        assert_matches!(mbatch.append(0, 3), PollResult::NotReady(Some(instant)) => assert_eq!(instant, expire_0));
        assert_matches!(mbatch.append(1, 2), PollResult::NotReady(Some(instant)) => assert_eq!(instant, expire_0));
        assert_matches!(mbatch.poll(), PollResult::NotReady(Some(instant)) => assert_eq!(instant, expire_0));

        assert_matches!(mbatch.append(0, 4), PollResult::Ready(0, drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        assert_matches!(mbatch.append(1, 3), PollResult::NotReady(Some(instant)) => assert!(instant > expire_0));
        assert_matches!(mbatch.poll(), PollResult::NotReady(Some(instant)) => assert!(instant > expire_0));
    }

    #[test]
    fn test_drain_stream() {
        let mut mbatch = MultistreamBatch::new(4, Duration::from_secs(10));

        assert_matches!(mbatch.append(0, 1), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(0, 2), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(0, 3), PollResult::NotReady(Some(_instant)));

        assert_matches!(mbatch.append(1, 1), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(1, 2), PollResult::NotReady(Some(_instant)));

        assert_matches!(mbatch.drain_stream(1), Some((1, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        );

        assert_matches!(mbatch.drain_stream(0), Some((0, drain)) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3])
        );

        assert_matches!(mbatch.append(0, 5), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(0, 6), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(0, 7), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(0, 8), PollResult::Ready(0, drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [5, 6, 7, 8])
        );
    }

    #[test]
    fn test_flush() {
        let mut mbatch = MultistreamBatch::new(4, Duration::from_secs(10));

        assert_matches!(mbatch.append(0, 1), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(1, 1), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(0, 2), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(1, 2), PollResult::NotReady(Some(_instant)));
        assert_matches!(mbatch.append(0, 3), PollResult::NotReady(Some(_instant)));


        let batches = mbatch.flush();

        assert_eq!(batches[0].0, 0);
        assert_eq!(batches[0].1.as_slice(), [1, 2, 3]);

        assert_eq!(batches[1].0, 1);
        assert_eq!(batches[1].1.as_slice(), [1, 2]);
    }
}

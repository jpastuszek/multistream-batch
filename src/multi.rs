use std::fmt::Debug;
use std::time::{Duration, Instant};
use linked_hash_map::LinkedHashMap;
use std::hash::Hash;
use std::vec::Drain;

#[derive(Debug)]
struct StreamBatch<T: Debug> {
    items: Vec<T>,
    created: Instant,
}

impl<T: Debug> StreamBatch<T> {
    fn new(capacity: usize) -> StreamBatch<T> {
        StreamBatch {
            items: Vec::with_capacity(capacity),
            created: Instant::now(),
        }
    }

    fn from_cache(mut items: Vec<T>) -> StreamBatch<T> {
        items.clear();
        StreamBatch {
            items,
            created: Instant::now(),
        }
    }
}

#[derive(Debug)]
pub enum BatchResult<'a, K: Debug, T: Debug> {
    /// Batch is complete due to reaching one of the limits `Ready` variant contains key and
    /// `Drain` iterator for batch items.
    Ready(K, Drain<'a, T>),
    /// No batch is ready; optional `Duration` until first `max_duration` limit will be reached
    /// if there are outstanding batches.
    NotReady(Option<Duration>),
}

impl<'a, K: Debug, T: Debug> From<(K, Drain<'a, T>)> for BatchResult<'a, K, T> {
    fn from(kv: (K, Drain<'a, T>)) -> BatchResult<'a, K, T> {
        BatchResult::Ready(kv.0, kv.1)
    }
}

/// Crates batches based on key that are provided when they reach given limit.
/// Batches are cached to avoid allocations.
#[derive(Debug)]
pub struct MultistreamBatch<K: Debug + Ord + Hash, T: Debug> {
    max_size: usize,
    max_duration: Duration,
    // Cache of empty batches
    cache: Vec<Vec<T>>,
    // Batches that have items in them but has not yet reached any limit in order of insertion
    outstanding: LinkedHashMap<K, StreamBatch<T>>,
}

impl<K, T> MultistreamBatch<K, T> where K: Debug + Ord + Hash + Send + Clone + 'static, T: Debug {
    pub fn new(max_size: usize, max_duration: Duration) -> MultistreamBatch<K, T> {
        assert!(max_size > 1, "MultistreamBatch::new max_size needst to be more than 1");

        MultistreamBatch {
            max_size,
            max_duration,
            cache: Default::default(),
            outstanding: Default::default(),
        }
    }

    /// Drain batch and move it to cache.
    fn drain_batch(&mut self, key: K) -> (K, Drain<T>) {
        // Move items from outstanding to cache
        let items = self.outstanding.remove(&key).expect("bad drain key").items;
        self.cache.push(items);
        let items = self.cache.last_mut().unwrap();

        // Drain items
        let drain = items.drain(0..);
        (key, drain)
    }

    /// Flush all outstanding batches starting from oldest.
    pub fn flush(&mut self) -> Vec<(K, Vec<T>)> {
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

    /// Removes cached batch buffers.
    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }

    /// Insert next item into a batch with given stream key.
    ///
    /// Returns `BatchResult::Ready(K, Drain<T>)` where `K` is key of ready batch and `T`
    /// are the items in the batch.
    ///
    /// Drain will provide items from batch that:
    /// * `max_size` of the batch was reached,
    /// * `max_duration` since first element returned elapsed.
    ///
    /// Returns `BatchResult::NotReady(Option<Duration>)` when no batch has reached a limit with
    /// optional `Duration` of time until oldest batch reaches `max_duration` limit.
    pub fn insert(&mut self, item: Option<(K, T)>) -> BatchResult<K, T> {
        if let Some((key, item)) = item {
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
                return self.drain_batch(key).into()
            }
        }

        // Check oldest outstanding batch
        if let Some((key, batch)) = self.outstanding.back() {
            let since_start = Instant::now().duration_since(batch.created);
            let key = key.clone();

            // Reached max_duration limit
            if since_start > self.max_duration {
                return self.drain_batch(key).into()
            }

            return BatchResult::NotReady(Some(self.max_duration - since_start))
        }

        return BatchResult::NotReady(None)
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    use std::time::Duration;
    use assert_matches::assert_matches;

    #[test]
    fn test_batch_not_ready_no_duration() {
        let mut mbatch = MultistreamBatch::new(4, Duration::from_secs(10));

        // empty has no outstanding batches
        assert_matches!(mbatch.insert(None), BatchResult::NotReady(None));

        assert_matches!(mbatch.insert(Some((0, 1))), BatchResult::NotReady(Some(_duration)));

        // now we have outstanding
        assert_matches!(mbatch.insert(None), BatchResult::NotReady(Some(_duration)));

        assert_matches!(mbatch.insert(Some((0, 2))), BatchResult::NotReady(Some(_duration)));
        assert_matches!(mbatch.insert(Some((0, 3))), BatchResult::NotReady(Some(_duration)));
        assert_matches!(mbatch.insert(Some((0, 4))), BatchResult::Ready(0, drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        // no outstanding again
        assert_matches!(mbatch.insert(None), BatchResult::NotReady(None));
    }

    #[test]
    fn test_batch_streams() {
        let mut mbatch = MultistreamBatch::new(4, Duration::from_secs(10));

        assert_matches!(mbatch.insert(None), BatchResult::NotReady(None));

        assert_matches!(mbatch.insert(Some((0, 1))), BatchResult::NotReady(Some(_duration)));
        assert_matches!(mbatch.insert(Some((0, 2))), BatchResult::NotReady(Some(_duration)));
        assert_matches!(mbatch.insert(Some((0, 3))), BatchResult::NotReady(Some(_duration)));

        assert_matches!(mbatch.insert(Some((0, 4))), BatchResult::Ready(0, drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        assert_matches!(mbatch.insert(Some((0, 1))), BatchResult::NotReady(Some(_duration)));
        //sender.send((0, 2)).unwrap();
        //sender.send((0, 3)).unwrap();
        //sender.send((0, 4)).unwrap();
        //sender.send((0, 5)).unwrap();

        //let (key, batch) = mbatch.next().unwrap();
        //assert_eq!(key, 0);
        //let batch = batch.collect::<Vec<_>>();
        //assert_eq!(batch.as_slice(), [1, 2, 3, 4]);

        //sender.send((1, 1)).unwrap();
        //sender.send((0, 6)).unwrap();
        //sender.send((1, 2)).unwrap();
        //sender.send((0, 7)).unwrap();
        //sender.send((1, 3)).unwrap();
        //sender.send((1, 4)).unwrap();
        //sender.send((0, 8)).unwrap();
        //sender.send((1, 5)).unwrap();
        //sender.send((1, 6)).unwrap();
        //sender.send((1, 7)).unwrap();
        //sender.send((1, 8)).unwrap();

        //let (key, batch) = mbatch.next().unwrap();
        //assert_eq!(key, 1);
        //let batch = batch.collect::<Vec<_>>();
        //assert_eq!(batch.as_slice(), [1, 2, 3, 4]);

        //let (key, batch) = mbatch.next().unwrap();
        //assert_eq!(key, 0);
        //let batch = batch.collect::<Vec<_>>();
        //assert_eq!(batch.as_slice(), [5, 6, 7, 8]);

        //let (key, batch) = mbatch.next().unwrap();
        //assert_eq!(key, 1);
        //let batch = batch.collect::<Vec<_>>();
        //assert_eq!(batch.as_slice(), [5, 6, 7, 8]);
    }
}

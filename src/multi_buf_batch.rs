//! This module provides `MultiBufBatch` that will buffer items into multiple internal batches based on batch stream key until
//! one of the batches is ready and provides this items in one go along with the batch stream key using `Drain` iterator.
use linked_hash_map::LinkedHashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::{Duration, Instant};
use std::vec::Drain;

/// Represents outstanding batch with items buffer from cache and `Instant` at which it was crated.
#[derive(Debug)]
struct OutstandingBatch<I: Debug> {
    items: Vec<I>,
    created: Instant,
}

impl<I: Debug> OutstandingBatch<I> {
    fn new() -> OutstandingBatch<I> {
        OutstandingBatch {
            items: Vec::new(),
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

/// Represents result from `MultiBufBatch.poll()` function call.
#[derive(Debug)]
pub enum PollResult<K: Debug> {
    /// Batch `K` is ready after reaching one of the limits.
    Ready(K),
    /// No outstanding batch has not reached one of its limits yet.
    /// Provides `Duration` after which `max_duration` limit will be reached
    /// if there is at least one outstanding batch.
    NotReady(Option<Duration>),
}

/// Usage statistics.
#[derive(Debug)]
pub struct Stats {
    /// Number of outstanding batches.
    pub outstanding: usize,
    /// Number of cached buffers (not used by outstanding batches).
    pub cached_buffers: usize,
}

/// Collects items into multiple batches based on stream key.
/// A batch may become ready after collecting `max_size` number of items or until `max_duration` has elapsed
/// since first item was appended to the batch.
///
/// Batch item buffers are cached and reused to avoid allocations.
///
/// This base implementation does not handle actual awaiting for batch duration timeout.
#[derive(Debug)]
pub struct MultiBufBatch<K: Debug + Ord + Hash, I: Debug> {
    max_size: usize,
    max_duration: Duration,
    // Cache of empty batch item buffers
    cache: Vec<Vec<I>>,
    // Batches that have items in them but has not yet reached any limit in order of insertion
    outstanding: LinkedHashMap<K, OutstandingBatch<I>>,
    // Batch with key K is ready to be consumed due to reaching max_size limit
    full: Option<K>,
}

impl<K, I> MultiBufBatch<K, I>
where
    K: Debug + Ord + Hash + Clone,
    I: Debug,
{
    /// Crates new instance with given maximum batch size (`max_size`) and maximum duration (`max_duration`) that
    /// batch can last since first item appended to it.
    ///
    /// Panics if `max_size` == 0.
    pub fn new(max_size: usize, max_duration: Duration) -> MultiBufBatch<K, I> {
        assert!(max_size > 0, "MultiBufBatch::new bad max_size");

        MultiBufBatch {
            max_size,
            max_duration,
            cache: Default::default(),
            outstanding: Default::default(),
            full: Default::default(),
        }
    }

    /// Checks if batch has reached one of its limits.
    ///
    /// Returns:
    /// * `PollResult::Ready(K)` - batch for stream key `K` has reached one of its limit and is ready to be consumed,
    /// * `PollResult::NotReady(None)` - batch is not ready yet and has no items appeded yet,
    /// * `PollResult::NotReady(Some(duration))` - batch is not ready yet but it will be ready after time duration due to duration limit.
    pub fn poll(&self) -> PollResult<K> {
        // Check oldest full batch first to make sure that following call to append won't fail
        if let Some(key) = &self.full {
            return PollResult::Ready(key.clone());
        }

        // Check oldest outstanding batch
        if let Some((key, batch)) = self.outstanding.front() {
            let since_start = Instant::now().duration_since(batch.created);

            if since_start >= self.max_duration {
                return PollResult::Ready(key.clone());
            }

            return PollResult::NotReady(Some(self.max_duration - since_start));
        }

        return PollResult::NotReady(None);
    }

    /// Appends item to batch with given stream key.
    ///
    /// It is an contract error to append batch that is ready according to `self.poll()`.
    ///
    /// Panics if batch has already reached its `max_size` limit.
    pub fn append(&mut self, key: K, item: I) {
        assert!(
            self.full.is_none(),
            "MultiBufBatch::append unconsumed full batch"
        );

        // Look up batch in outstanding or crate one using cached or new items buffer
        if let Some(batch) = self.outstanding.get_mut(&key) {
            assert!(
                batch.items.len() < self.max_size,
                "MultiBufBatch::append on full batch"
            );

            batch.items.push(item);

            // Mark as full
            if batch.items.len() >= self.max_size {
                self.full = Some(key);
            }
        } else {
            let mut batch = if let Some(items) = self.cache.pop() {
                OutstandingBatch::from_cache(items)
            } else {
                OutstandingBatch::new()
            };

            batch.items.push(item);
            self.outstanding.insert(key, batch);
        }
    }

    /// Moves outstanding batch item buffer to cache and returns its `&mut` reference.
    fn move_to_cache(&mut self, key: &K) -> Option<&mut Vec<I>> {
        // If consuming full key clear it
        if self.full.as_ref().filter(|fkey| *fkey == key).is_some() {
            self.full.take();
        }

        // Move items from outstanding to cache
        let items = self.outstanding.remove(key)?.items;
        self.cache.push(items);
        self.cache.last_mut()
    }

    /// Lists keys of outstanding batches.
    pub fn outstanding(&self) -> impl Iterator<Item = &K> {
        self.outstanding.keys()
    }

    /// Starts new batch dropping all buffered items.
    pub fn clear(&mut self, key: &K) {
        self.move_to_cache(key).map(|items| items.clear());
    }

    /// Consumes batch by draining items from internal buffer.
    pub fn drain(&mut self, key: &K) -> Option<Drain<I>> {
        self.move_to_cache(key).map(|items| items.drain(0..))
    }

    /// Flushes all outstanding batches starting from oldest.
    pub fn flush(&mut self) -> Vec<(K, Vec<I>)> {
        let cache = &mut self.cache;
        let outstanding = &mut self.outstanding;

        outstanding
            .entries()
            .map(|entry| {
                let key = entry.key().clone();

                // Move to cache
                let items = entry.remove().items;
                cache.push(items);
                let items = cache.last_mut().unwrap();

                // Move items out preserving capacity
                let items = items.split_off(0);

                (key, items)
            })
            .collect()
    }

    /// Returns slice of internal item buffer of given outstanding batch.
    pub fn get(&self, key: &K) -> Option<&[I]> {
        self.outstanding
            .get(key)
            .map(|batch| batch.items.as_slice())
    }

    /// Drops cached batch buffers.
    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }

    /// Provides usage statistics.
    pub fn stats(&self) -> Stats {
        Stats {
            outstanding: self.outstanding.len(),
            cached_buffers: self.cache.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    use assert_matches::assert_matches;
    use std::time::Duration;

    #[test]
    fn test_batch_poll() {
        let mut batch = MultiBufBatch::new(4, Duration::from_secs(10));

        // empty has no outstanding batches
        assert_matches!(batch.poll(), PollResult::NotReady(None));

        batch.append(0, 1);

        // now we have outstanding
        assert_matches!(batch.poll(), PollResult::NotReady(Some(_instant)));

        batch.append(0, 2);
        batch.append(0, 3);
        batch.append(0, 4);

        assert_matches!(batch.poll(), PollResult::Ready(0) =>
            assert_eq!(batch.drain(&0).unwrap().collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        // no outstanding again
        assert_matches!(batch.poll(), PollResult::NotReady(None));
    }

    #[test]
    fn test_batch_max_size() {
        let mut batch = MultiBufBatch::new(4, Duration::from_secs(10));

        batch.append(0, 1);
        batch.append(0, 2);
        batch.append(0, 3);
        batch.append(0, 4);

        assert_matches!(batch.poll(), PollResult::Ready(0) =>
            assert_eq!(batch.drain(&0).unwrap().collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        batch.append(0, 5);
        batch.append(0, 6);
        batch.append(0, 7);
        batch.append(0, 8);

        assert_matches!(batch.poll(), PollResult::Ready(0) =>
            assert_eq!(batch.drain(&0).unwrap().collect::<Vec<_>>().as_slice(), [5, 6, 7, 8])
        );

        batch.append(1, 1);
        batch.append(0, 9);
        batch.append(1, 2);
        batch.append(0, 10);
        batch.append(1, 3);
        batch.append(0, 11);
        batch.append(1, 4);

        assert_matches!(batch.poll(), PollResult::Ready(1) =>
            assert_eq!(batch.drain(&1).unwrap().collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        batch.append(0, 12);

        assert_matches!(batch.poll(), PollResult::Ready(0) =>
            assert_eq!(batch.drain(&0).unwrap().collect::<Vec<_>>().as_slice(), [9, 10, 11, 12])
        );
    }

    #[test]
    fn test_batch_max_duration() {
        let mut batch = MultiBufBatch::new(4, Duration::from_millis(100));

        batch.append(0, 1);
        batch.append(0, 2);

        let ready_after = match batch.poll() {
            PollResult::NotReady(Some(ready_after)) => ready_after,
            _ => panic!("expected NotReady with instant"),
        };

        std::thread::sleep(ready_after);

        assert_matches!(batch.poll(), PollResult::Ready(0) =>
            assert_eq!(batch.drain(&0).unwrap().collect::<Vec<_>>().as_slice(), [1, 2])
        );

        batch.append(0, 3);
        batch.append(0, 4);
        batch.append(0, 5);
        batch.append(0, 6);

        assert_matches!(batch.poll(), PollResult::Ready(0) =>
            assert_eq!(batch.drain(&0).unwrap().collect::<Vec<_>>().as_slice(), [3, 4, 5, 6])
        );
    }

    #[test]
    fn test_drain_stream() {
        let mut batch = MultiBufBatch::new(4, Duration::from_secs(10));

        batch.append(0, 1);
        batch.append(0, 2);
        batch.append(0, 3);

        batch.append(1, 1);
        batch.append(1, 2);

        assert_matches!(batch.drain(&1), Some(drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        );

        assert_matches!(batch.drain(&0), Some(drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3])
        );

        batch.append(0, 5);
        batch.append(0, 6);
        batch.append(0, 7);
        batch.append(0, 8);

        assert_matches!(batch.poll(), PollResult::Ready(0) =>
            assert_eq!(batch.drain(&0).unwrap().collect::<Vec<_>>().as_slice(), [5, 6, 7, 8])
        );
    }

    #[test]
    fn test_flush() {
        let mut batch = MultiBufBatch::new(4, Duration::from_secs(10));

        batch.append(0, 1);
        batch.append(1, 1);
        batch.append(0, 2);
        batch.append(1, 2);
        batch.append(0, 3);

        let batches = batch.flush();

        assert_eq!(batches[0].0, 0);
        assert_eq!(batches[0].1.as_slice(), [1, 2, 3]);

        assert_eq!(batches[1].0, 1);
        assert_eq!(batches[1].1.as_slice(), [1, 2]);
    }
}

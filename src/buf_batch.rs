//! This module provides `BufBatch` that will buffer items until the batch is ready and provide them in
//! one go using `Drain` iterator.
use std::fmt::Debug;
use std::time::{Duration, Instant};
use std::vec::Drain;

/// Represents result from the `poll` function call.
#[derive(Debug)]
pub enum PollResult {
    /// Batch is ready after reaching one of the limits.
    Ready,
    /// Batch has not reached one of its limits yet.
    /// Provides `Duration` after which `max_duration` limit will be reached if the batch has
    /// at least one item.
    NotReady(Option<Duration>),
}

/// Batches items in internal buffer up to `max_size` items or until `max_duration` has elapsed
/// since the first item appended to the batch.
///
/// This base implementation does not handle actual awaiting for batch duration timeout.
#[derive(Debug)]
pub struct BufBatch<I: Debug> {
    items: Vec<I>,
    first_item: Option<Instant>,
    max_size: usize,
    max_duration: Duration,
}

impl<I: Debug> BufBatch<I> {
    /// Creates batch given maximum batch size in the number of items stored (`max_size`)
    /// and maximum duration that batch can last (`max_duration`) since the first item appended to it.
    ///
    /// Panics if `max_size == 0`.
    pub fn new(max_size: usize, max_duration: Duration) -> BufBatch<I> {
        assert!(max_size > 0, "BufBatch::new bad max_size");

        BufBatch {
            items: Vec::new(),
            first_item: None,
            max_size,
            max_duration,
        }
    }

    /// Checks if the batch has reached one of its limits.
    ///
    /// Returns:
    /// * `PollResult::Ready` - batch has reached one of its limits and is ready to be consumed,
    /// * `PollResult::NotReady(None)` - batch is not ready yet and has no items appended yet,
    /// * `PollResult::NotReady(Some(duration))` - batch is not ready yet but it will be ready after time duration due to duration limit.
    pub fn poll(&self) -> PollResult {
        debug_assert!(self.items.is_empty() ^ self.first_item.is_some());

        if self.items.len() >= self.max_size {
            return PollResult::Ready;
        }

        if let Some(first_item) = self.first_item {
            let since_start = Instant::now().duration_since(first_item);

            if since_start >= self.max_duration {
                return PollResult::Ready;
            }

            return PollResult::NotReady(Some(self.max_duration - since_start));
        }
        PollResult::NotReady(None)
    }

    /// Appends item to batch and returns a reference to that item.
    ///
    /// It is a contract error to append batch that is ready according to `self.poll()`.
    ///
    /// Panics if the batch has already reached its `max_size` limit.
    pub fn append(&mut self, item: I) -> &I {
        debug_assert!(self.items.is_empty() ^ self.first_item.is_some());
        assert!(
            self.items.len() < self.max_size,
            "BufBatch::append on full batch"
        );

        // Count `max_duration` from first item inserted
        self.first_item.get_or_insert_with(|| Instant::now());

        self.items.push(item);
        self.items.last().unwrap()
    }

    /// Starts a new batch by dropping all buffered items.
    pub fn clear(&mut self) {
        self.first_item = None;
        self.items.clear();
    }

    /// Starts a new batch by draining all buffered items.
    pub fn drain(&mut self) -> Drain<I> {
        self.first_item = None;
        self.items.drain(0..)
    }

    /// Pops last item from internal buffer.
    pub fn pop(&mut self) -> Option<I> {
        if self.items.len() == 1 {
            self.first_item = None;
        }
        self.items.pop()
    }

    /// Removes item from internal buffer at index.
    ///
    /// Panics if index is out of bounds.
    pub fn remove(&mut self, index: usize) -> I {
        if self.items.len() == 1 {
            self.first_item = None;
        }
        self.items.remove(index)
    }

    /// Converts into internal item buffer.
    pub fn into_vec(self) -> Vec<I> {
        self.items
    }

    /// Returns slice of internal item buffer.
    pub fn as_slice(&self) -> &[I] {
        self.items.as_slice()
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    use assert_matches::assert_matches;
    use std::time::Duration;

    #[test]
    fn test_batch_poll() {
        let mut batch = BufBatch::new(4, Duration::from_secs(10));

        // Empty has no outstanding batches
        assert_matches!(batch.poll(), PollResult::NotReady(None));

        batch.append(1);

        // Now we have outstanding
        assert_matches!(batch.poll(), PollResult::NotReady(Some(_instant)));

        batch.append(2);
        batch.append(3);
        batch.append(4);

        assert_matches!(batch.poll(), PollResult::Ready =>
            assert_eq!(batch.drain().collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        // No outstanding again
        assert_matches!(batch.poll(), PollResult::NotReady(None));
    }

    #[test]
    fn test_batch_max_size() {
        let mut batch = BufBatch::new(4, Duration::from_secs(10));

        batch.append(1);
        batch.append(2);
        batch.append(3);
        batch.append(4);

        assert_matches!(batch.poll(), PollResult::Ready =>
            assert_eq!(batch.drain().collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
        );

        batch.append(5);
        batch.append(6);
        batch.append(7);
        batch.append(8);

        assert_matches!(batch.poll(), PollResult::Ready =>
            assert_eq!(batch.drain().collect::<Vec<_>>().as_slice(), [5, 6, 7, 8])
        );
    }

    #[test]
    fn test_batch_max_duration() {
        let mut batch = BufBatch::new(4, Duration::from_millis(100));

        batch.append(1);
        batch.append(2);

        let ready_after = match batch.poll() {
            PollResult::NotReady(Some(ready_after)) => ready_after,
            _ => panic!("expected NotReady with instant"),
        };

        std::thread::sleep(ready_after);

        assert_matches!(batch.poll(), PollResult::Ready =>
            assert_eq!(batch.drain().collect::<Vec<_>>().as_slice(), [1, 2])
        );

        batch.append(3);
        batch.append(4);
        batch.append(5);
        batch.append(6);

        assert_matches!(batch.poll(), PollResult::Ready =>
            assert_eq!(batch.drain().collect::<Vec<_>>().as_slice(), [3, 4, 5, 6])
        );
    }

    #[test]
    fn test_drain_stream() {
        let mut batch = BufBatch::new(4, Duration::from_secs(10));

        batch.append(1);
        batch.append(2);
        batch.append(3);

        assert_eq!(batch.drain().collect::<Vec<_>>().as_slice(), [1, 2, 3]);

        batch.append(1);
        batch.append(2);

        assert_eq!(batch.drain().collect::<Vec<_>>().as_slice(), [1, 2]);

        batch.append(5);
        batch.append(6);
        batch.append(7);
        batch.append(8);

        assert_matches!(batch.poll(), PollResult::Ready =>
            assert_eq!(batch.drain().collect::<Vec<_>>().as_slice(), [5, 6, 7, 8])
        );
    }
}

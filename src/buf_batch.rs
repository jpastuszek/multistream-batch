use std::time::{Duration, Instant};
use std::fmt::Debug;
use std::vec::Drain;

mod channel;
pub use channel::*;

/// Represents result from `poll` and `append` functions where batch is `Ready` to be consumed or `NotReady` yet.
#[derive(Debug)]
pub enum PollResult {
    Ready,
    NotReady(Option<Duration>),
}

/// Represents outstanding batch with items buffer from cache and `Instant` at which it was crated.
#[derive(Debug)]
pub struct BufBatch<I: Debug> {
    items: Vec<I>,
    first_item: Option<Instant>,
    max_size: usize,
    max_duration: Duration,
}

impl<I: Debug> BufBatch<I> {
    /// Creates batch given maximum batch size in number of items (`max_size`)
    /// and maximum duration that batch can last (`max_duration`) since first item appended to it. 
    pub fn new(max_size: usize, max_duration: Duration) -> BufBatch<I> {
        assert!(max_size > 0, "BufBatch::new/from_vec bad max_size");

        BufBatch {
            items: Vec::new(),
            first_item: None,
            max_size,
            max_duration,
        }
    }

    /// Checks if batch has reached one of its limits.
    /// 
    /// Returns:
    /// * `PollResult::Ready` - batch has reached one of its limit and is ready to be consumed,
    /// * `PollNotReady(Some(duration))` - batch is not ready yet but it will be ready after duration due to duration limit,
    /// * `PollNotReady(None)` batch is not ready yet and has not received its first item.
    pub fn poll(&self) -> PollResult {
        debug_assert!(self.items.is_empty() ^ self.first_item.is_some());

        if self.items.len() >= self.max_size {
            return PollResult::Ready
        }

        if let Some(first_item) = self.first_item {
            let since_start = Instant::now().duration_since(first_item);

            if since_start >= self.max_duration {
                return PollResult::Ready
            }

            return PollResult::NotReady(Some(self.max_duration - since_start))
        }
        PollResult::NotReady(None)
    }

    /// Appends item to batch and returns reference to that item.
    /// 
    /// It is an contract error to append batch that is ready according to `self.poll()`.
    /// 
    /// Panics if batch has already reached its `max_size` limit.
    pub fn append(&mut self, item: I) -> &I {
        debug_assert!(self.items.is_empty() ^ self.first_item.is_some());

        if self.items.len() >= self.max_size {
            panic!("BufBatch append on full batch");
        }

        // Count `max_duration` from first item inserted
        self.first_item.get_or_insert_with(|| Instant::now());

        self.items.push(item);
        self.items.last().unwrap()
    }

    /// Starts new batch dropping all buffered items.
    pub fn clear(&mut self) {
        self.first_item = None;
        self.items.clear();
    }

    /// Starts new batch by draining all buffered items.
    pub fn drain(&mut self) -> Drain<I> {
        self.first_item = None;
        self.items.drain(0..)
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
    use std::time::Duration;
    use assert_matches::assert_matches;

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
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
    /// Create batch given maximum batch size in number of items (`max_size`)
    /// and maximum duration a batch can last (`max_duration`) since first item appended to it. 
    pub fn new(max_size: usize, max_duration: Duration) -> BufBatch<I> {
        Self::from_vec(max_size, max_duration, Vec::with_capacity(max_size))
    }

    /// Reuse existing `Vec` as item buffer.
    pub fn from_vec(max_size: usize, max_duration: Duration, mut items: Vec<I>) -> BufBatch<I> {
        // Make sure nothing is left after undrained
        items.clear();

        assert!(max_size > 0, "BufBatch::new/from_vec bad max_size");

        BufBatch {
            items,
            first_item: None,
            max_size,
            max_duration,
        }
    }

    /// Start new batch dropping all buffered items.
    pub fn clear(&mut self) {
        self.first_item = None;
        self.items.clear();
    }

    /// Consume batch by copying items to newly allocated `Vec`.
    pub fn split_off(&mut self) -> Vec<I> {
        self.first_item = None;
        self.items.split_off(0)
    }

    /// Consume batch by draining items from internal buffer.
    pub fn drain(&mut self) -> Drain<I> {
        self.first_item = None;
        self.items.drain(0..)
    }

    /// Consume batch by swapping items buffer with given `Vec` and clear
    pub fn swap(&mut self, items: &mut Vec<I>) {
        std::mem::swap(&mut self.items, items);
        self.clear();
    }

    /// Convert into internal item buffer
    pub fn into_vec(self) -> Vec<I> {
        self.items
    }

    /// Return slice from internal item buffer
    pub fn as_slice(&self) -> &[I] {
        self.items.as_slice()
    }

    /// Check if batch has reached one of its limits.
    /// 
    /// Returns:
    /// * `PollResult::Ready` if batch has reached one of its limit and is ready to be consumed.
    /// * `PollNotReady(Some(duration))` if it is not ready yet but it will be ready after duration due to duration limit.
    /// * `PollNotReady(None)` if it is not ready yet and has not received its first item.
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

    /// Appends item to batch and returns reference to item just inserted.
    /// 
    /// It is a contract error to append batch that is ready according to `poll()`.
    /// Panics if trying to append a batch that reached its `max_size` limit.
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
}
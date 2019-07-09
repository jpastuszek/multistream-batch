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
    /// `max_duration` since this batch was crated or reset
    pub fn new(max_size: usize, max_duration: Duration) -> BufBatch<I> {
        Self::from_vec(max_size, max_duration, Vec::with_capacity(max_size))
    }

    /// Reuse existing `Vec`
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

    /// Clear items and start new batch
    pub fn clear(&mut self) {
        self.first_item = None;
        self.items.clear();
    }

    /// Return items as new `Vec` and start new batch
    pub fn split_off(&mut self) -> Vec<I> {
        self.first_item = None;
        self.items.split_off(0)
    }

    /// Drain items from internal buffer and start new batch
    /// Assuming that `Drain` iterator is not leaked leading to stale items left in items buffer.
    pub fn drain(&mut self) -> Drain<I> {
        self.first_item = None;
        self.items.drain(0..)
    }

    /// Swap items buffer with given `Vec` and clear
    pub fn swap(&mut self, items: &mut Vec<I>) {
        std::mem::swap(&mut self.items, items);
        self.clear();
    }

    /// Convert into intrnal item buffer
    pub fn into_vec(self) -> Vec<I> {
        self.items
    }

    /// Return slice from intranl item buffer
    pub fn as_slice(&self) -> &[I] {
        self.items.as_slice()
    }

    /// Check if batch has reached one of its limits.
    /// 
    /// Retruns `PollResult::Ready` if batch has reached its one of its limit.
    /// Retruns `PollNotReady(Some(duration))` if it is not ready yet but it will be ready after duration due to duration limit.
    /// Retruns `PollNotReady(None)` if it is not ready yet and has not received its first item.
    pub fn poll(&self) -> PollResult {
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

    /// Appends item to batch and returns reference to item just inserted or `Err(I)` indicating that batch has reached its max_size limit.
    /// 
    /// Note that `poll()` needs to be called before append to determine if batch has reached its duration limit. This function will not check
    /// for duration limit as this woudl potentially race with call to `poll()`.
    pub fn append(&mut self, item: I) -> Result<&I, I> {
        if self.items.len() >= self.max_size {
            return Err(item)
        }

        self.items.push(item);
        Ok(self.items.last().unwrap())
    }
}
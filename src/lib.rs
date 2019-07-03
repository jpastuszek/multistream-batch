use crossbeam_channel::{Sender, Receiver, RecvTimeoutError};

use std::fmt::Debug;
use std::time::{Duration, Instant};
use std::error::Error;
use std::fmt;

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct EndOfStreamError;

impl fmt::Display for EndOfStreamError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "no more entries will be provided to this batch")
    }
}

impl Error for EndOfStreamError {}

#[derive(Debug)]
pub struct Batch<T: Debug> {
    channel: Receiver<T>,
    items: Vec<T>,
    cursor: usize,
    max_size: usize,
    max_duration: Duration,
    disconnected: bool,
    batch_start: Option<Instant>,
}

impl<T: Debug> Batch<T> {
    pub fn new(max_size: usize, max_duration: Duration) -> (Sender<T>, Batch<T>) {
        let (sender, receiver) = crossbeam_channel::bounded(max_size * 2);

        (sender, Batch {
            channel: receiver,
            items: Vec::with_capacity(max_size),
            cursor: 0,
            max_size,
            max_duration,
            disconnected: false,
            batch_start: None,
        })
    }

    pub fn with_producer_thread(max_size: usize, max_duration: Duration, producer: impl Fn(Sender<T>) -> () + Send + 'static) -> Batch<T> where T: Send + 'static {
        let (sender, batch) = Batch::new(max_size, max_duration);

        std::thread::spawn(move || {
            producer(sender)
        });

        batch
    }

    /// Get next item from the batch.
    ///
    /// Returns `None` signaling end of batch if:
    /// * `max_size` of the batch has been reached,
    /// * `max_duration` since first element returned has elapsed.
    ///
    /// This call will block indefinitely waiting for first item of the batch.
    ///
    /// After calling `retry` this will provide batch items again from the first one. It will
    /// continue fetching items from producer until max_size or max_duration is reached starting
    /// with original first item time.
    ///
    /// After calling `commit` this function will behave as if new `Batch` object was crated.
    pub fn next(&mut self) -> Option<&T> {
        // Yield internal messages
        if self.cursor < self.items.len() {
            let e = &self.items[self.cursor];
            self.cursor += 1;
            return Some(e)
        }

        // Reached max_size limit
        if self.cursor == self.max_size {
            return None
        }

        let recv = match self.batch_start {
            // Wait indefinitely for first item that will start the batch
            None => match self.channel.recv() {
                Ok(e) => {
                    // Got first item - record batch start time
                    self.batch_start.get_or_insert_with(|| Instant::now());
                    Ok(e)
                },
                Err(_) => Err(EndOfStreamError),
            }
            // Wait for timeout for next item of the batch
            Some(batch_start) => {
                let since_start = Instant::now().duration_since(batch_start);

                // Reached max_duration
                if since_start > self.max_duration {
                    return None
                }

                match self.channel.recv_timeout(self.max_duration - since_start) {
                    Ok(e) => Ok(e),
                    // Reached max_duration limit
                    Err(RecvTimeoutError::Timeout) => return None,
                    // Other end gone
                    Err(RecvTimeoutError::Disconnected) => Err(EndOfStreamError),
                }
            }
        };

        match recv {
            Ok(e) => {
                self.items.push(e);
                self.cursor += 1;
                return Some(self.items.last().unwrap())
            }
            Err(EndOfStreamError) => {
                // Let the batch process and we notify that we got disconnected on commit
                self.disconnected = true;
                return None
            }
        }
    }

    pub fn commit(&mut self) -> Result<(), EndOfStreamError> {
        if self.disconnected {
            return Err(EndOfStreamError)
        }

        self.items.clear();
        self.cursor = 0;
        self.batch_start = None;
        Ok(())
    }

    pub fn retry(&mut self) {
        self.cursor = 0;
    }

    pub fn uncommitted(&self) -> usize {
        self.items.len()
    }
}

#[derive(Debug)]
pub struct StreamBatch<T: Debug> {
    items: Vec<T>,
    cursor: usize,
    batch_start: Option<Instant>,
}

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

struct MultistreamBatch<K: Ord, T: Debug> {
    channel: Receiver<T>,
    disconnected: bool,
    max_size: usize,
    max_duration: Duration,
    batches: HashMap<K, StreamBatch<T>>,
    active: BTreeMap<Instant, K>,
}

impl<K: Ord + Hash, T: Debug> MultistreamBatch<K, T> {
    pub fn new(max_size: usize, max_duration: Duration) -> (Sender<T>, MultistreamBatch<K, T>) {
        let (sender, receiver) = crossbeam_channel::bounded(max_size * 2);

        (sender, MultistreamBatch {
            channel: receiver,
            max_size,
            max_duration,
            disconnected: false,
            batches: Default::default(),
            active: Default::default(),
        })
    }

    pub fn with_producer_thread(max_size: usize, max_duration: Duration, producer: impl Fn(Sender<T>) -> () + Send + 'static) -> MultistreamBatch<K, T> where K: Ord, T: Send + 'static {
        let (sender, batch) = MultistreamBatch::new(max_size, max_duration);

        std::thread::spawn(move || {
            producer(sender)
        });

        batch
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    use std::time::Duration;

    #[test]
    fn test_batch_reset() {
        let (sender, mut batch) = Batch::new(4, Duration::from_secs(10));

        sender.send(1).unwrap();
        sender.send(2).unwrap();
        sender.send(3).unwrap();
        sender.send(4).unwrap();
        sender.send(5).unwrap();

        assert_eq!(*batch.next().unwrap(), 1);
        assert_eq!(*batch.next().unwrap(), 2);
        assert_eq!(*batch.next().unwrap(), 3);

        batch.retry();

        assert_eq!(*batch.next().unwrap(), 1);
        assert_eq!(*batch.next().unwrap(), 2);
        assert_eq!(*batch.next().unwrap(), 3);

        batch.retry();

        assert_eq!(*batch.next().unwrap(), 1);

        batch.retry();

        assert_eq!(*batch.next().unwrap(), 1);
        assert_eq!(*batch.next().unwrap(), 2);
        assert_eq!(*batch.next().unwrap(), 3);
        assert_eq!(*batch.next().unwrap(), 4);
        assert!(batch.next().is_none()); // max_size
    }

    #[test]
    fn test_batch_commit() {
        let (sender, mut batch) = Batch::new(2, Duration::from_secs(10));

        sender.send(1).unwrap();
        sender.send(2).unwrap();
        sender.send(3).unwrap();
        sender.send(4).unwrap();

        assert_eq!(*batch.next().unwrap(), 1);
        assert_eq!(*batch.next().unwrap(), 2);
        assert!(batch.next().is_none()); // max_size

        batch.commit().unwrap();

        assert_eq!(*batch.next().unwrap(), 3);

        batch.retry();

        assert_eq!(*batch.next().unwrap(), 3);
        assert_eq!(*batch.next().unwrap(), 4);
        assert!(batch.next().is_none()); // max_size
    }

    #[test]
    fn test_batch_with_producer_thread() {
        let mut batch = Batch::with_producer_thread(2, Duration::from_secs(10), |sender| {
            sender.send(1).unwrap();
            sender.send(2).unwrap();
            sender.send(3).unwrap();
            sender.send(4).unwrap();
        });

        assert_eq!(*batch.next().unwrap(), 1);
        assert_eq!(*batch.next().unwrap(), 2);
        assert!(batch.next().is_none()); // max_size

        batch.commit().unwrap();

        assert_eq!(*batch.next().unwrap(), 3);

        batch.retry();

        assert_eq!(*batch.next().unwrap(), 3);
        assert_eq!(*batch.next().unwrap(), 4);
        assert!(batch.next().is_none()); // max_size
    }

    #[test]
    fn test_batch_max_duration() {
        let mut batch = Batch::with_producer_thread(2, Duration::from_millis(100), |sender| {
            sender.send(1).unwrap();
            std::thread::sleep(Duration::from_millis(500));
        });

        assert_eq!(*batch.next().unwrap(), 1);
        assert!(batch.next().is_none()); // max_duration
    }

    #[test]
    fn test_batch_disconnected() {
        let mut batch = Batch::with_producer_thread(2, Duration::from_secs(10), |sender| {
            sender.send(1).unwrap();
        });

        assert_eq!(*batch.next().unwrap(), 1);
        assert!(batch.next().is_none()); // disconnected

        assert!(batch.commit().is_err()); // disconnected
    }
}

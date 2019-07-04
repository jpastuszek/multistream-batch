use crossbeam_channel::{Sender, Receiver, RecvTimeoutError};
use crate::EndOfStreamError;

use std::fmt::Debug;
use std::time::{Duration, Instant};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::Hash;
use std::collections::vec_deque::Drain;

#[derive(Debug)]
struct StreamBatch<T: Debug> {
    items: VecDeque<T>,
}

impl<T: Debug> StreamBatch<T> {
    fn new() -> StreamBatch<T> {
        StreamBatch {
            items: VecDeque::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn len(&self) -> usize {
        self.items.len()
    }

    fn push(&mut self, item: T) {
        self.items.push_back(item)
    }

    fn drain(&mut self) -> Drain<T> {
        self.items.drain(0..)
    }
}

// Should I use Rc<RefCell<StreamBatch>>> instead of K + lookup?
struct MultistreamBatch<K: Ord, T: Debug> {
    channel: Receiver<(K, T)>,
    disconnected: bool,
    max_size: usize,
    max_duration: Duration,
    // All known batches by stream key
    batches: HashMap<K, StreamBatch<T>>,
    // Stream key of batches that have received at least one message and are not complete yet
    outstanding: VecDeque<(Instant, K)>,
}

impl<K, T> MultistreamBatch<K, T> where K: Ord + Hash + Send + Clone + 'static, T: Debug {
    pub fn new(max_size: usize, max_duration: Duration) -> (Sender<(K, T)>, MultistreamBatch<K, T>) {
        let (sender, receiver) = crossbeam_channel::bounded(max_size * 2);
        assert!(max_size > 0, "MultistreamBatch::new max_size == 0");

        (sender, MultistreamBatch {
            channel: receiver,
            max_size,
            max_duration,
            disconnected: false,
            batches: Default::default(),
            outstanding: Default::default(),
        })
    }

    pub fn with_producer_thread(max_size: usize, max_duration: Duration, producer: impl Fn(Sender<(K, T)>) -> () + Send + 'static) -> MultistreamBatch<K, T> where K: Ord, T: Send + 'static {
        let (sender, batch) = MultistreamBatch::new(max_size, max_duration);

        std::thread::spawn(move || {
            producer(sender)
        });

        batch
    }

    fn drain_batch(&mut self, key: K) -> (K, Drain<T>) {
        let drain = self.batches.get_mut(&key).expect("outstanding key but batch missing").drain();
        (key, drain)
    }

    /// Get next item from completed batch of any stream.
    ///
    /// Returns `Ok(Some((K, T)))` where `K` is key of the current batch being streamed and `T` is the
    /// next item of the batch.
    ///
    /// Returns `Ok(None)` signaling end of batch if:
    /// * `max_size` of the batch was reached,
    /// * `max_duration` since first element returned elapsed.
    ///
    /// This call will block indefinitely waiting for first item if no batches are outstanding.
    ///
    /// If sending end has been dropped/colsed `Err(EndOfStreamError)` will be returned after flushing all
    /// outstading batches starting from oldest.
    pub fn next(&mut self) -> Result<(K, Drain<T>), EndOfStreamError> {
        loop {
            // Channel is disconnected
            if self.disconnected {
                // Flusth outstanding batches starting from oldest
                if let Some((_start, key)) = self.outstanding.pop_front() {
                    return Ok(self.drain_batch(key))
                }
                // If no more batches left free memory and return EndOfStreamError
                self.batches.clear();
                return Err(EndOfStreamError);
            }


            // If we have outstanding batches recv_with_timeout based on oldest batch interval - max_duration
            let kitem = if let Some((batch_start, key)) = self.outstanding.pop_front() {
                let since_start = Instant::now().duration_since(batch_start);

                // Reached max_duration limit
                if since_start > self.max_duration {
                    return Ok(self.drain_batch(key))
                }

                match self.channel.recv_timeout(self.max_duration - since_start) {
                    Ok(kitem) => {
                        // Reschedule as oldest
                        self.outstanding.push_front((batch_start, key));
                        Some(kitem)
                    },
                    // Reached max_duration limit
                    Err(RecvTimeoutError::Timeout) => {
                        return Ok(self.drain_batch(key))
                    }
                    // Other end gone
                    Err(RecvTimeoutError::Disconnected) => None,
                }
            } else {
                // If we have no outstanding batches blocking recv() from channel
                match self.channel.recv() {
                    Ok(kitem) => Some(kitem),
                    // Other end gone
                    Err(_) => None,
                }
            };

            // On message look up batch by key and add to batch
            if let Some((key, item)) = kitem {
                let batch = self.batches.entry(key.clone()).or_insert_with(|| StreamBatch::new());

                // If batch had no message before insert it to outstanding with now instant
                if batch.is_empty() {
                    self.outstanding.push_back((Instant::now(), key.clone()));
                }

                batch.push(item);

                // Reached max_size limit
                if batch.len() >= self.max_size {
                    return Ok(self.drain_batch(key))
                }
            } else {
                // If we got channel closed mark disconnet
                self.disconnected = true;
                continue;
            }
        }
    }
}

use crossbeam_channel::{Sender, Receiver, RecvTimeoutError};
use crate::EndOfStreamError;

use std::fmt::Debug;
use std::time::{Duration, Instant};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::Hash;

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

    fn next(&mut self) -> Option<T> {
        self.items.pop_front()
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn push(&mut self, item: T) {
        self.items.push_back(item)
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
    live: VecDeque<(Instant, K)>,
    // Complete batch that we are now streaming
    complete: Option<(K, StreamBatch<T>)>,
}

impl<K, T> MultistreamBatch<K, T> where K: Ord + Hash + Send + Clone + 'static, T: Debug {
    pub fn new(max_size: usize, max_duration: Duration) -> (Sender<(K, T)>, MultistreamBatch<K, T>) {
        let (sender, receiver) = crossbeam_channel::bounded(max_size * 2);

        (sender, MultistreamBatch {
            channel: receiver,
            max_size,
            max_duration,
            disconnected: false,
            batches: Default::default(),
            live: Default::default(),
            complete: None,
        })
    }

    pub fn with_producer_thread(max_size: usize, max_duration: Duration, producer: impl Fn(Sender<(K, T)>) -> () + Send + 'static) -> MultistreamBatch<K, T> where K: Ord, T: Send + 'static {
        let (sender, batch) = MultistreamBatch::new(max_size, max_duration);

        std::thread::spawn(move || {
            producer(sender)
        });

        batch
    }

    fn move_complete(&mut self, key: K) {
        let batch = self.batches.remove(&key).expect("live key but batch missing");
        self.complete = Some((key, batch));
    }

    pub fn next(&mut self) -> Result<Option<T>, EndOfStreamError> {

        loop {
            // check if complete is some and stream it untill done
            if let Some((key, mut batch)) = self.complete.take() {
                let item = batch.next();
                if item.is_some() {
                    self.complete = Some((key, batch));
                    return Ok(item);
                }
                // put it back to batches when done and return None
                self.batches.insert(key, batch);
                return Ok(None);
            }

            // if disconnected
            //  * take first batch from batches as compelte
            //  * if no more batches left return EndOfStreamError
            if self.disconnected {
                // flusth live batches starting from oldest
                if let Some((_start, key)) = self.live.pop_front() {
                    self.move_complete(key);
                    continue;
                }
                // free memory
                self.batches.clear();
                return Err(EndOfStreamError);
            }


            // if we have live batches recv_with_timeout based on oldest batch interval - max_duration
            let kitem = if let Some((batch_start, key)) = self.live.pop_front() {
                let since_start = Instant::now().duration_since(batch_start);

                // Reached max_duration limit
                if since_start > self.max_duration {
                    self.move_complete(key);
                    continue;
                }

                match self.channel.recv_timeout(self.max_duration - since_start) {
                    Ok(kitem) => {
                        // reschedule
                        self.live.push_front((batch_start, key));
                        Some(kitem)
                    },
                    // Reached max_duration limit
                    Err(RecvTimeoutError::Timeout) => {
                        self.move_complete(key);
                        continue;
                    }
                    // Other end gone
                    Err(RecvTimeoutError::Disconnected) => None,
                }
            } else {
                // if we have no live batches blocking recv() from channel
                match self.channel.recv() {
                    Ok(kitem) => Some(kitem),
                    // Other end gone
                    Err(_) => None,
                }
            };

            // on message look up batch by key and add to batch
            if let Some((key, item)) = kitem {
                let batch = self.batches.entry(key.clone()).or_insert_with(|| StreamBatch::new());

                // if batch had no message before insert it to live with now instant
                if batch.is_empty() {
                    self.live.push_back((Instant::now(), key));
                }

                batch.push(item);
            } else {
                // if we got channel closed mark disconnet
                self.disconnected = true;
                continue;
            }
        }
    }
}

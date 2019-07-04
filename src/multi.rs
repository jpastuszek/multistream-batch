use crossbeam_channel::{Sender, Receiver, RecvTimeoutError};

use std::fmt::Debug;
use std::time::{Duration, Instant};
use std::error::Error;
use std::fmt;

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

use crossbeam_channel::{Sender, Receiver, RecvTimeoutError};
use crate::EndOfStreamError;
use super::{MultistreamBatch, PollResult};

use std::hash::Hash;
use std::fmt::Debug;
use std::time::{Duration, Instant};
use std::vec::Drain;

#[derive(Debug)]
pub struct MultistreamBatchChannel<K: Debug + Ord + Hash, T: Debug> {
    channel: Receiver<(K, T)>,
    mbatch: MultistreamBatch<K, T>,
    flush: Option<Vec<(K, Vec<T>)>>,
    flush_index: usize,
    // Instant at after which we can poll batch
    next_batch_at: Option<Instant>,
}

impl<K, T> MultistreamBatchChannel<K, T> where K: Debug + Ord + Hash + Send + Clone + 'static, T: Debug + Send + 'static {
    pub fn new(max_size: usize, max_duration: Duration) -> (Sender<(K, T)>, MultistreamBatchChannel<K, T>) {
        let (sender, receiver) = crossbeam_channel::bounded(max_size * 2);

        (sender, MultistreamBatchChannel {
            channel: receiver,
            mbatch: MultistreamBatch::new(max_size, max_duration),
            flush: None,
            flush_index: 0,
            next_batch_at: None,
        })
    }

    pub fn with_producer_thread(max_size: usize, max_duration: Duration, producer: impl Fn(Sender<(K, T)>) -> () + Send + 'static) -> MultistreamBatchChannel<K, T> {
        let (sender, batch) = MultistreamBatchChannel::new(max_size, max_duration);

        std::thread::spawn(move || {
            producer(sender)
        });

        batch
    }

    /// Get next batch as pair of key and drain iterator for items.
    ///
    /// Call may block until item is received or batch duration limit was reached.
    ///
    /// Retruns `Ok(None)` if no batch was ready at this call, call again.
    /// Returns `Err(EndOfStreamError)` after `Sender` end was dropped and all outstanding batches
    /// were flushed.
    pub fn next<'i>(&'i mut self) -> Result<Option<(K, Drain<'i, T>)>, EndOfStreamError> {
        if self.flush.is_some() {
            // Note that I can't use if let above or we move mut borrow here
            let batches = self.flush.as_mut().unwrap();

            if self.flush_index >= batches.len() {
                // We are done flushing, free memory and bail
                batches.clear();
                return Err(EndOfStreamError);
            }

            let (key, items) = &mut batches[self.flush_index];
            self.flush_index += 1;
            return Ok(Some((key.clone(), items.drain(0..))))
        }

        let now = Instant::now();

        // Check if batch is ready due to duration limit
        let poll_ready = self.next_batch_at.map(|instant| instant > now).unwrap_or(false);

        if poll_ready {
            // We should have ready batch but if not update next_batch_at and go again
            match self.mbatch.poll() {
                PollResult::Ready(key, drain) => return Ok(Some((key, drain))),
                PollResult::NotReady(instant) => {
                    // Update instant here as batch could have been already returned by insert
                    self.next_batch_at = instant;
                    return Ok(None)
                }
            }
        }

        let item = if let Some(instant) = self.next_batch_at {
            match self.channel.recv_timeout(now.duration_since(instant)) {
                Ok(item) => Ok(item),
                // A batch should be ready, go again
                Err(RecvTimeoutError::Timeout) => return Ok(None),
                // Other end gone
                Err(RecvTimeoutError::Disconnected) => Err(EndOfStreamError),
            }
        } else {
            // No outstanding batches so wait for first item
            self.channel.recv().map_err(|_| EndOfStreamError)
        };

        match item {
            Ok((key, item)) => match self.mbatch.insert(key, item) {
                PollResult::Ready(key, drain) => return Ok(Some((key, drain))),
                PollResult::NotReady(instant) => {
                    self.next_batch_at = instant;
                    return Ok(None)
                }
            },
            Err(_eos) => {
                // Flush batches and free memory
                let batches = self.mbatch.flush();
                self.mbatch.clear_cache();
                self.flush = Some(batches);

                // There won't be next batch
                self.next_batch_at.take();

                return Ok(None)
            }
        }
    }

    /* requires polonius: https://github.com/rust-lang/rust/issues/54663
    pub fn next<'i>(&'i mut self) -> Result<(K, Drain<'i, T>), EndOfStreamError> {
        if let Some(batches) = self.flush.as_mut() {
            if self.flush_index >= batches.len() {
                // Free memory
                batches.clear();
                return Err(EndOfStreamError);
            }

            let (key, items) = &mut batches[self.flush_index];
            self.flush_index += 1;
            return Ok((key.clone(), items.drain(0..)))
        }

        loop {
            let item = match self.mbatch.poll() {
                PollResult::Ready(key, drain) => return Ok((key, drain)),
                PollResult::NotReady(None) => self.channel.recv().map_err(|_| EndOfStreamError),
                PollResult::NotReady(Some(mut instant)) => {
                    let now = Instant::now();
                    // race
                    if now < instant {
                        instant = now;
                    }

                    match self.channel.recv_timeout(now.duration_since(instant)) {
                        Ok(item) => Ok(item),
                        // A batch should have reached max_duration limit try again
                        Err(RecvTimeoutError::Timeout) => continue,
                        // Other end gone
                        Err(RecvTimeoutError::Disconnected) => Err(EndOfStreamError),
                    }
                }
            };

            match item {
                Ok((key, item)) => match self.mbatch.insert(key, item) {
                    PollResult::Ready(key, drain) => return Ok((key, drain)),
                    PollResult::NotReady(_) => continue,
                },
                Err(_eos) => {
                    let batches = self.mbatch.flush();
                    self.mbatch.clear_cache();
                    self.flush = Some(batches);
                    continue;
                }
            }
        }
    }
    */
}

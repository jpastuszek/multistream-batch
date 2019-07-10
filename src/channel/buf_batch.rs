use crossbeam_channel::{Sender, Receiver, RecvTimeoutError};
use crate::channel::EndOfStreamError;
use crate::buf_batch::{PollResult, BufBatch};

use std::fmt::Debug;
use std::time::Duration;
use std::vec::Drain;

/// Commands that can be send to `BufBatchChannel` via `Sender` endpoint.
#[derive(Debug)]
pub enum Command<I: Debug> {
    /// Append item `I` to batch.
    Append(I),
    /// Flush outstanding items.
    Complete,
}

#[derive(Debug)]
pub struct BufBatchChannel<I: Debug> {
    channel: Receiver<Command<I>>,
    batch: BufBatch<I>,
    // True when channel is disconnected
    disconnected: bool,
}

impl<I: Debug> BufBatchChannel<I> {
    /// Creates batch given maximum batch size in number of items (`max_size`)
    /// and maximum duration that batch can last (`max_duration`) since first item appended to it. 
    pub fn new(max_size: usize, max_duration: Duration, channel_size: usize) -> (Sender<Command<I>>, BufBatchChannel<I>) {
        let (sender, receiver) = crossbeam_channel::bounded(channel_size);

        (sender, BufBatchChannel {
            channel: receiver,
            batch: BufBatch::new(max_size, max_duration),
            disconnected: false,
        })
    }

    /// Crates batch calling `producer` closure with `Sender` end of the channel in newly started thread.
    pub fn with_producer_thread(max_size: usize, max_duration: Duration, channel_size: usize, producer: impl Fn(Sender<Command<I>>) -> () + Send + 'static) -> BufBatchChannel<I> where I: Send + 'static {
        let (sender, batch) = BufBatchChannel::new(max_size, max_duration, channel_size);

        std::thread::spawn(move || {
            producer(sender)
        });

        batch
    }

    /// Gets next item from the batch.
    ///
    /// Returns `Ok(BatchResult::Item(I))` with next item of the batch.
    ///
    /// Returns `Ok(BatchResult::Complete)` signaling end of batch if:
    /// * `max_size` of the batch was reached,
    /// * `max_duration` since first element returned elapsed,
    /// * client sent `Command::Complete`.
    /// 
    /// Caller is responsible for consuming the batch via provided methods or calling `clear()`.
    ///
    /// This call will block indefinitely waiting for first item of the batch.
    ///
    /// After calling `retry` this will provide batch items again from the first one. It will
    /// continue fetching items from producer until max_size or max_duration is reached starting
    /// with original first item time.
    ///
    /// After calling `clear` this function will behave as if new `Batch` object was started.
    pub fn next(&mut self) -> Result<Drain<I>, EndOfStreamError> {
        if self.disconnected {
            return Err(EndOfStreamError)
        }

        loop {
            // Check if we have a ready batch due to any limit or go fetch next item
            let ready_after = match self.batch.poll() {
                PollResult::Ready => return Ok(self.batch.drain()),
                PollResult::NotReady(ready_after) => ready_after,
            };

            let recv_result = if let Some(ready_after) = ready_after {
                match self.channel.recv_timeout(ready_after) {
                    // We got new item before timeout was reached
                    Ok(item) => Ok(item),
                    // A batch should be ready now; try again
                    Err(RecvTimeoutError::Timeout) => continue,
                    // Other end gone
                    Err(RecvTimeoutError::Disconnected) => Err(EndOfStreamError),
                }
            } else {
                // No outstanding batches; wait for first item
                self.channel.recv().map_err(|_| EndOfStreamError)
            };

            match recv_result {
                Ok(Command::Complete) => {
                    // Mark as complete by producer
                    return Ok(self.batch.drain())
                },
                Ok(Command::Append(item)) => {
                    self.batch.append(item);
                    continue
                }
                Err(_eos) => {
                    self.disconnected = true;
                    return Ok(self.batch.drain())
                }
            };
        }
    }

    /// Checks if previous `self.next()` call found channel to be disconnected.
    pub fn is_disconnected(&self) -> bool {
        self.disconnected
    }

    /// Starts new batch dropping all buffered items.
    pub fn clear(&mut self) {
        self.batch.clear()
    }

    /// Starts new batch by draining all buffered items.
    pub fn drain(&mut self) -> Drain<I> {
        self.batch.drain()
    }

    /// Converts into internal item buffer.
    pub fn into_vec(self) -> Vec<I> {
        self.batch.into_vec()
    }

    /// Returns slice of internal item buffer.
    pub fn as_slice(&self) -> &[I] {
        self.batch.as_slice()
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    use std::time::Duration;
    use assert_matches::assert_matches;

    #[test]
    fn test_batch_max_size() {
        let (sender, mut batch) = BufBatchChannel::new(2, Duration::from_secs(10), 10);

        sender.send(Command::Append(1)).unwrap();
        sender.send(Command::Append(2)).unwrap();
        sender.send(Command::Append(3)).unwrap();

        assert_matches!(batch.next(), Ok(drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        ); // max_size
    }

    #[test]
    fn test_batch_with_producer_thread() {
        let mut batch = BufBatchChannel::with_producer_thread(2, Duration::from_secs(10), 10, |sender| {
            sender.send(Command::Append(1)).unwrap();
            sender.send(Command::Append(2)).unwrap();
            sender.send(Command::Append(3)).unwrap();
            sender.send(Command::Append(4)).unwrap();
        });

        assert_matches!(batch.next(), Ok(drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2])
        ); // max_size

        assert_matches!(batch.next(), Ok(drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [3, 4])
        ); // max_size
    }

    #[test]
    fn test_batch_max_duration() {
        let mut batch = BufBatchChannel::with_producer_thread(2, Duration::from_millis(100), 10, |sender| {
            sender.send(Command::Append(1)).unwrap();
            std::thread::sleep(Duration::from_millis(500));
        });

        assert_matches!(batch.next(), Ok(drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1])
        ); // max_duration
        assert!(!batch.is_disconnected()); // check if Complete result was not because thread has finished
    }

    #[test]
    fn test_batch_disconnected() {
        let mut batch = BufBatchChannel::with_producer_thread(2, Duration::from_secs(10), 10, |sender| {
            sender.send(Command::Append(1)).unwrap();
        });

        assert_matches!(batch.next(), Ok(drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1])
        ); // disconnected
        assert_matches!(batch.next(), Err(EndOfStreamError));
    }

    #[test]
    fn test_batch_command_complete() {
        let mut batch = BufBatchChannel::with_producer_thread(2, Duration::from_secs(10), 10, |sender| {
            sender.send(Command::Append(1)).unwrap();
            sender.send(Command::Complete).unwrap();
            sender.send(Command::Append(2)).unwrap();
            sender.send(Command::Complete).unwrap();
        });

        assert_matches!(batch.next(), Ok(drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1])
        ); // command

        assert_matches!(batch.next(), Ok(drain) =>
            assert_eq!(drain.collect::<Vec<_>>().as_slice(), [2])
        ); // command
    }
}
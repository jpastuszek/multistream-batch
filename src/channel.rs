//! Batch implementations that use channels and threads to support simultaneously receiving items and awaiting on timeouts.
//!
//! These implementations are using `crossbeam_channel` to implement awaiting for items or timeout.

pub mod buf_batch;
pub mod multi_buf_batch;
pub mod tx_buf_batch;

use std::error::Error;
use std::fmt;

/// The error that is returned by channel based implementations when `Sender` end of
/// the channel was dropped and no more outstanding items are left to be provided.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct EndOfStreamError;

impl fmt::Display for EndOfStreamError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "no more items will be provided to this batch")
    }
}

impl Error for EndOfStreamError {}

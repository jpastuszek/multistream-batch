pub mod buf_batch;
pub mod multi_buf_batch;
pub mod tx_batch;

use std::error::Error;
use std::fmt;

/// Error returned by channel based implementations when `Sender` end of 
/// channel was dropped and no more outstanding data is left to be provided. 
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct EndOfStreamError;

impl fmt::Display for EndOfStreamError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "no more entries will be provided to this batch")
    }
}

impl Error for EndOfStreamError {}
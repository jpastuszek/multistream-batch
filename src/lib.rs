mod tx_batch;
pub use tx_batch::*;
mod multi;
pub use multi::*;

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

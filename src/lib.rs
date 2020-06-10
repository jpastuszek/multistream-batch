/*!
Implementations of batching algorithms.

Batching works by accumulating items and later automatically flushing them all together when the batch has reached a limit.
All items collected in the single batch are available at once for further processing (e.g. batch insert into a database).

These implementations will construct batches based on:
* limit of the number of items collected in a batch,
* limit of time duration since the first item appended to the batch,
* calling one of the batch consuming methods,
* sending flush command between batch items (channel-based implementations).

See sub modules for documentation of available algorithms.
!*/

pub mod buf_batch;
#[cfg(feature = "crossbeam-channel")]
pub mod channel;
pub mod multi_buf_batch;

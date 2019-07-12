/*!
This Rust library provides different types and implementations of batching algorithms.

Batching is based on collecting items and flushing them all together when batch has reached some limit or when manually flushed. This makes all items collected in single batch available at once for further processing (e.g. batch insert into a database).

This implementations will construct batches based on:
* maximum number of items collected,
* maximum time duration since first item was collected by the batch,
* calling one of the batch consuming methods,
* sending flush command between batch items (channel based batches).

See sub modules for documentation of available algorithms.
!*/

pub mod buf_batch;
pub mod multi_buf_batch;
#[cfg(feature = "crossbeam-channel")]
pub mod channel;

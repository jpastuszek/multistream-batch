[![Latest Version]][crates.io] [![Documentation]][docs.rs] ![License]

Rust library that provides different types and implementations of batching algorithms.

Batching is based on collecting items and flushing them all together when batch has reached some limit or when manually flushed. This makes all items collected in single batch available at once for further processing (e.g. batch insert into a database).

This implementations will construct batches based on:
* maximum number of items collected,
* maximum time duration since first item was collected by the batch,
* calling one of the batch consuming methods,
* sending flush command between batch items (channel based batches).

See [documentation](https://docs.rs/multistream-batch) of available algorithms.

# Example

Collect batches of items from two streams by reaching different individual batch limits and using `Flush` command.

```rust
use multistream_batch::channel::multi_buf_batch::MultiBufBatchChannel;
use multistream_batch::channel::multi_buf_batch::Command::*;
use std::time::Duration;
use assert_matches::assert_matches;

// Create producer thread and batcher with maximum size of 4 items (for each stream) and
// maximum batch duration since first received item of 200 ms.
let mut batch = MultiBufBatchChannel::with_producer_thread(4, Duration::from_millis(200), 10, |sender| {
	// Send a sequence of `Append` commands with integer stream key and item value
	sender.send(Append(1, 1)).unwrap();
	sender.send(Append(0, 1)).unwrap();
	sender.send(Append(1, 2)).unwrap();
	sender.send(Append(0, 2)).unwrap();
	sender.send(Append(1, 3)).unwrap();
	sender.send(Append(0, 3)).unwrap();
	sender.send(Append(1, 4)).unwrap();
	// At this point batch with stream key `1` should have reached its capacity of 4 items
	sender.send(Append(0, 4)).unwrap();
	// At this point batch with stream key `0` should have reached its capacity of 4 items

	// Send some more to buffer up for next batch
	sender.send(Append(0, 5)).unwrap();
	sender.send(Append(1, 5)).unwrap();
	sender.send(Append(1, 6)).unwrap();
	sender.send(Append(0, 6)).unwrap();

	// Introduce delay to trigger maximum duration timeout
	std::thread::sleep(Duration::from_millis(400));

	// Send items that will be flushed by `Flush` command
	sender.send(Append(0, 7)).unwrap();
	sender.send(Append(1, 7)).unwrap();
	sender.send(Append(1, 8)).unwrap();
	sender.send(Append(0, 8)).unwrap();
	// Flush outstanding items for batch with stream key `1` and `0`
	sender.send(Flush(1)).unwrap();
	sender.send(Flush(0)).unwrap();

	// Last buffered up items will be flushed automatically when this thread exits
	sender.send(Append(0, 9)).unwrap();
	sender.send(Append(1, 9)).unwrap();
	sender.send(Append(1, 10)).unwrap();
	sender.send(Append(0, 10)).unwrap();
	// Exiting closure will shutdown the producer thread
});

// Batches flushed due to individual batch size limit
assert_matches!(batch.next(), Ok((1, drain)) =>
	assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
);

assert_matches!(batch.next(), Ok((0, drain)) =>
	assert_eq!(drain.collect::<Vec<_>>().as_slice(), [1, 2, 3, 4])
);

// Batches flushed due to duration limit
assert_matches!(batch.next(), Ok((0, drain)) =>
	assert_eq!(drain.collect::<Vec<_>>().as_slice(), [5, 6])
);

assert_matches!(batch.next(), Ok((1, drain)) =>
	assert_eq!(drain.collect::<Vec<_>>().as_slice(), [5, 6])
);

// Batches flushed by sending `Flush` command starting from batch with stream key `1`
assert_matches!(batch.next(), Ok((1, drain)) =>
	assert_eq!(drain.collect::<Vec<_>>().as_slice(), [7, 8])
);

assert_matches!(batch.next(), Ok((0, drain)) =>
	assert_eq!(drain.collect::<Vec<_>>().as_slice(), [7, 8])
);

// Batches flushed by dropping sender (thread exit)
assert_matches!(batch.next(), Ok((0, drain)) =>
	assert_eq!(drain.collect::<Vec<_>>().as_slice(), [9, 10])
);

assert_matches!(batch.next(), Ok((1, drain)) =>
	assert_eq!(drain.collect::<Vec<_>>().as_slice(), [9, 10])
);
```

[crates.io]: https://crates.io/crates/multistream-batch
[Latest Version]: https://img.shields.io/crates/v/multistream-batch.svg
[Documentation]: https://docs.rs/multistream-batch/badge.svg
[docs.rs]: https://docs.rs/multistream-batch
[License]: https://img.shields.io/crates/l/multistream-batch.svg

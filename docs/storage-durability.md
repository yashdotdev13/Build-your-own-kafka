# Storage Durability Guarantee

## Append semantics

A successful `LogSegment.append()` writes the record's offset, value length, and payload to the segment file and calls `FileChannel.force(true)` before returning.

This requests that the file contents and associated metadata be forced to the underlying storage device. The guarantee depends on the operating system, filesystem, storage hardware, and their handling of persistence requests.

## Resource lifecycle

The segment file is opened for each append and the channel is closed using try-with-resources. This avoids retaining an open file handle between append operations.

## Recovery

The segment can be reopened to reconstruct its record count and read previously written records. Recovery validates record headers, lengths, and payload completeness.

The reopen tests demonstrate recovery after normal writes and reopening. They do not simulate a power failure, operating-system crash, or hardware failure.

## Performance trade-off

Forcing every append adds I/O latency compared with merely flushing a Java output stream. This policy favors durability over maximum write throughput. A future batching or configurable durability policy would need separate design and testing.

## Failure limitations

If a write or force operation fails, `append()` reports a runtime exception. A failed append is not transactional: a partial record may already have reached the file. Recovery detects incomplete records, but automatic truncation or repair is not guaranteed by this policy.

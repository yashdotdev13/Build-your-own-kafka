# Storage Engine

## Overview

Build-your-own-kafka stores records in partition logs on disk. The
storage components are responsible for appending records, reading them
back, organizing log data into segments, and recovering the next offset
when the broker starts.

The implementation is a simplified file-based storage engine for
learning. It is not the same storage implementation used internally by
Apache Kafka.

## Storage Hierarchy

Records are organized through the following hierarchy:

``` text
Topic
  |
  +-- Partition
        |
        +-- PartitionLog
              |
              +-- LogSegment
                    |
                    +-- Record
```

A topic contains partitions. Each partition maintains its own log, and
the log manages its storage segments. Records are stored in the log in
offset order.

## Records and Offsets

A record contains an offset and a value. The offset identifies the
record's position within its partition.

When a partition appends a record, it uses its current `nextOffset`,
writes the record to the log, and then advances the next offset. This
produces sequential offsets for successful appends within that
partition.

Offsets are partition-local. They do not represent a single global
sequence across all topics or partitions.

## Partition Log

The partition delegates storage operations to its `PartitionLog`.

The log is responsible for writing records to the active segment and
reading records from the stored log data. Keeping these operations
behind the log abstraction allows the partition to focus on offset
assignment and partition-level behavior.

## Log Segments

As a log grows, it is divided into segments rather than being
represented as one indefinitely growing file.

The active segment receives new records. When the configured
segment-rotation condition is reached, the log can rotate to a new
segment.

Segmentation helps organize growing log data and provides a foundation
for future storage operations. The current project should not be assumed
to implement all of Kafka's segment indexing, retention, compaction, or
recovery optimizations.

## Append Flow

A successful append follows this general sequence:

1.  The partition creates a record using its next offset.
2.  The partition log appends the record to its active segment.
3.  After the append succeeds, the partition advances its next offset.
4.  The appended record is returned to the caller.

The partition's append method is synchronized so concurrent calls do not
assign the same next offset through that method.

## Reading Records

A fetch operation specifies a starting offset. The partition log reads
records from the stored data beginning at that position, while the
partition and broker request path enforce the requested fetch behavior
and limits.

This makes it possible for a consumer to continue reading from a known
offset rather than loading the entire log into application memory.

## Persistence and Recovery

The broker persists log data to disk so records can be recovered after a
restart.

When a partition is initialized, it obtains the next offset from its log
using the log's recovered state. This allows the partition to continue
assigning offsets after previously stored records.

Recovery behavior depends on the log implementation and the data that
can be read from disk. The project should not claim the same
crash-consistency or corruption-recovery guarantees as Apache Kafka
unless those guarantees are explicitly implemented and tested.

## Batch Appends and Storage

Batch production sends several values for the same topic partition in
one request. The broker appends those values sequentially through the
partition.

The batch handler reads the starting offset and appends the batch while
holding the partition monitor. This keeps other synchronized partition
operations from interleaving during the batch append sequence.

This is not equivalent to a durable transactional write. The current
batch path does not provide rollback if a later append fails after
earlier records have already been written.

## What This Storage Engine Does Not Claim

The current storage engine is an educational implementation. Do not
assume it provides:

-   Replicated storage across brokers.
-   Kafka-compatible log or index file formats.
-   Log compaction or configurable retention.
-   Kafka's complete crash-recovery and durability guarantees.
-   Transactional or atomic batch semantics.

These are separate capabilities that would require additional design and
implementation.

## Related Documentation

-   [System Architecture](architecture.md)
-   [Communication Protocol](protocol.md)
-   [Broker Internals](broker.md)
-   [Producer](producer.md)
-   [Consumer](consumer.md)

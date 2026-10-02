# Producer

## Overview

The producer is the client-side component that sends records to the
broker. In Build-your-own-kafka, producer requests identify a topic and
partition and carry record values encoded in the project's custom
protocol.

The current implementation supports both single-record production and
batch production.

## Producer Request Flow

A producer operation follows this general flow:

1.  Select the target topic and partition.
2.  Create the appropriate produce payload.
3.  Encode the payload into the request format.
4.  Create a request with the relevant request type and correlation ID.
5.  Send the request over the TCP connection.
6.  Read and decode the broker response.
7.  Check the response status and decode the successful response
    payload.

The client and broker use the custom protocol described in
[Communication Protocol](protocol.md).

## Single-Record Production

Single-record production sends one record value in a `PRODUCE` request.

The payload contains:

-   Topic name.
-   Partition ID.
-   Record value.

The broker's produce handler decodes the payload, finds the topic and
partition, and appends the value to the partition. The partition assigns
the record's offset. The response contains that offset.

This operation is useful when the caller wants to send records
individually.

## Batch Production

Batch production allows multiple record values for the same topic
partition to be sent in one request.

The batch payload contains:

-   Topic name.
-   Partition ID.
-   Record count.
-   Each record's length and value.

The broker handles the batch by appending the records sequentially to
the selected partition. The response contains the batch's base offset
and record count.

For example, if a batch contains three records and the broker returns
base offset `10` with count `3`, the assigned offsets are `10`, `11`,
and `12`.

## Why Batch Production Exists

Without batching, a client needs to issue one produce request per
record. A batch groups multiple records into a single request, reducing
request overhead for workloads that send multiple records to the same
partition.

Batching in this project is explicit: the caller constructs a batch
payload and sends a `PRODUCE_BATCH` request. It should not be confused
with an asynchronous producer that automatically buffers records,
applies linger timeouts, or groups records across partitions.

## Batch Append Behavior

The broker's batch handler:

1.  Decodes the batch payload.
2.  Resolves the requested topic and partition.
3.  Reads the partition's next offset as the base offset.
4.  Appends each record in payload order.
5.  Returns the base offset and record count.

The handler synchronizes on the partition during the base-offset read
and append sequence, preventing other synchronized partition operations
from interleaving in that critical section.

The current implementation does not provide rollback if an append fails
partway through. Therefore, the response shape describes a successful
batch operation; it should not be interpreted as a general transaction
or atomic durability guarantee.

## Response Handling

For a single-record request, the successful response contains the
assigned offset.

For a batch request, the successful response contains:

-   **Base offset:** the offset assigned to the first record.
-   **Record count:** the number of records in the batch.

The client can use these fields to determine the offset range assigned
to a successfully produced batch.

If the broker returns an error status, the client should handle the
error payload rather than decode it as a success response.

## Current Scope

The producer functionality currently documented here includes:

-   Single-record produce requests.
-   Explicit batch produce requests.
-   Topic and partition selection in the request.
-   Broker-assigned sequential offsets.
-   Produce responses containing assigned offset information.

The current feature is a foundation for a more convenient producer-side
API. Automatic batching, retry policies, acknowledgments with
configurable durability, compression, idempotence, and transactional
production should not be assumed unless they are separately implemented.

## Related Documentation

-   [System Architecture](architecture.md)
-   [Communication Protocol](protocol.md)
-   [Broker Internals](broker.md)
-   [Storage Engine](storage-engine.md)
-   [Consumer](consumer.md)

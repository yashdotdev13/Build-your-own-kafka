# System Architecture

## Overview

Build-your-own-kafka is an educational, Kafka-inspired message broker
implemented in Java 21. The project is built from the ground up to
explore client-broker communication, record storage, and consumer
operations.

The current implementation is a simplified, single-broker system. It
borrows core concepts from Apache Kafka but does not reproduce Kafka's
complete distributed architecture or production guarantees.

## High-Level Components

The codebase is organized into three main areas:

-   **Client** --- connects to the broker and sends requests or receives
    responses. Client-side functionality includes producer and consumer
    operations.
-   **Protocol** --- defines the binary request and response format used
    for communication between clients and the broker.
-   **Broker** --- accepts client connections, dispatches requests to
    handlers, manages topics and partitions, stores records, and
    coordinates consumer-group operations.

``` text
+---------------------+
|       Client        |
| Producer / Consumer |
+----------+----------+
           |
           | TCP connection
           | framed protocol messages
           v
+---------------------+
|       Broker        |
|                     |
| Connection handling |
| Request dispatch    |
| Request handlers    |
+----------+----------+
           |
           v
+---------------------+
|   Topic Manager     |
| Topics / Partitions |
+----------+----------+
           |
           v
+---------------------+
|   Partition Logs    |
| Records / Segments  |
| Persistent storage  |
+---------------------+
```

This is a conceptual view of the main request path. Consumer-group
coordination is another broker responsibility and interacts with
consumer requests and partition assignments.

## Request Lifecycle

A typical client request follows this path:

1.  The client creates a request containing a request type, correlation
    ID, and encoded payload.
2.  The client sends the request over its TCP connection to the broker.
3.  The broker reads the framed message and reconstructs the request.
4.  The request dispatcher selects a handler based on the request type.
5.  The handler performs the operation using broker components such as
    the topic manager, partitions, logs, or group coordinator.
6.  The broker sends a response containing the correlation ID, status,
    and response payload.
7.  The client decodes the response and continues its operation.

The correlation ID helps associate a response with the request that
produced it.

## Topics, Partitions, and Records

Records are organized under topics. A topic contains partitions, and
each partition maintains its own ordered log.

A partition assigns sequential offsets to appended records. These
offsets identify records within that partition; they are not global
offsets across the entire broker.

The partition log is persisted using files and segmented as the log
grows. On startup, the implementation can recover stored records and
continue from the next offset.

## Producer Flow

The producer sends records to a topic and partition through the
protocol.

The implementation supports:

-   **Single-record production** --- a request appends one record and
    returns its assigned offset.
-   **Batch production** --- a request carries multiple records for the
    same topic and partition. The broker appends the records in sequence
    and returns the batch's base offset and record count.

Batch production reduces the need to send a separate request for every
record. The batch response describes the contiguous range of offsets
assigned to that batch.

## Consumer Flow

Consumers fetch records from a topic partition beginning at a requested
offset. The broker returns records up to the requested fetch limit.

The consumer implementation also supports offset-related operations.
Consumer groups add coordination on top of individual fetching,
including group membership, partition assignment, heartbeats, and
rebalancing.

## Scope and Limitations

This project is intended to make broker internals understandable through
a working implementation. Its current design should be understood within
that scope:

-   It is a simplified single-broker implementation, not a distributed
    Kafka cluster.
-   It does not provide Kafka's full replication, controller, or
    fault-tolerance architecture.
-   Its custom protocol is project-specific and is not wire-compatible
    with Apache Kafka.
-   Its behavior and guarantees are limited to the features implemented
    in this repository.

## Related Documentation

-   [Protocol](protocol.md)
-   [Broker](broker.md)
-   [Storage Engine](storage-engine.md)
-   [Producer](producer.md)
-   [Consumer](consumer.md)
-   [Consumer Groups](consumer-groups.md)

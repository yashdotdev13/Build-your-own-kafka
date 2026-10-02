# Broker Internals

## Overview

The broker is the server-side component of Build-your-own-kafka. It
accepts client connections, interprets requests, executes broker
operations, and returns responses.

The broker package contains the request-handling path as well as the
core objects used to manage topics, partitions, records, and
consumer-group coordination.

This document describes the broker responsibilities at a component
level. Exact behavior is determined by the implementation in the
repository.

## Broker Server and Client Connections

The broker listens for client TCP connections on its configured port. In
the current development setup, the broker has been exercised on port
`9092`.

A connection handler manages communication for an accepted client
connection. The broker uses a persistent connection model, allowing a
client to send multiple requests over the same connection.

The server uses Java virtual threads for connection handling, as part of
the project's concurrency approach.

## Request Dispatching

The broker separates receiving requests from executing individual
operations.

The request path is:

1.  Read a framed request from the client connection.
2.  Decode the bytes into a request object.
3.  Pass the request to the dispatcher.
4.  Select the handler registered for the request type.
5.  Execute the handler and produce a response.
6.  Encode and send the response to the client.

Handlers exist for operations including ping, topic creation, record
production and fetching, offset operations, consumer-group operations,
heartbeats, and metadata.

## Topic Management

The topic manager is responsible for looking up and managing topics.

A topic groups one or more partitions. Requests that operate on records
identify the target topic and partition. The broker resolves those
objects before performing the requested operation.

Topic and partition lookup also provides a place to validate that the
requested resources exist and that partition identifiers are valid.

## Partitions

A partition is an ordered sequence of records. It owns the next offset
to assign and delegates record persistence to its log.

When a record is appended, the partition:

1.  Creates a record using its current next offset.
2.  Appends the record to the partition log.
3.  Advances the next offset.
4.  Returns the appended record.

The partition's append operation is synchronized, protecting its offset
assignment and append sequence from concurrent calls to that operation.

Offsets are scoped to a partition. Two different partitions may both
contain a record with offset `0`.

## Record Production

The broker supports two produce paths:

-   **Single-record produce:** the handler decodes a produce payload,
    resolves the target partition, appends one record, and returns the
    assigned offset.
-   **Batch produce:** the handler decodes a batch payload, resolves the
    target partition, appends the batch's records in order, and returns
    the base offset and record count.

The batch handler synchronizes on the partition while reading the base
offset and appending the batch. This prevents another synchronized
partition operation from interleaving within that critical section.

The batch path is not a general transaction system. In particular, the
current implementation should not be described as providing rollback if
an append fails partway through a batch.

## Record Fetching

Fetch handlers resolve the requested topic and partition, then read
records beginning at the requested offset, subject to the fetch limit
supplied by the request.

The partition delegates reading to its log. This keeps the broker's
request-handling logic separate from the details of the log's storage
representation.

## Consumer-Group Coordination

The broker also manages consumer-group operations. The implemented flow
includes group joining and synchronization, leaving a group, heartbeats,
membership tracking, generation validation, and partition
assignment/rebalancing behavior.

These operations allow consumers to coordinate which group member is
responsible for a partition. They are distinct from the underlying
partition log and record storage.

## Metadata

The metadata request allows a client to query broker-managed topic and
partition information. This provides clients with information about the
resources available in the current broker implementation.

Metadata support should be understood within the project's single-broker
scope; it is not equivalent to Kafka's cluster metadata and controller
mechanisms.

## Responsibilities and Boundaries

The broker coordinates operations but does not define every concern
itself:

-   The **protocol** package defines request, response, and payload
    formats.
-   The **broker** package resolves resources and applies operation
    logic.
-   The **partition and log** components manage record ordering,
    offsets, and persistence.
-   The **client** package provides client-side communication and
    producer/consumer behavior.

## Scope and Limitations

Build-your-own-kafka is a simplified educational broker. It does not
implement the full Apache Kafka architecture, including multi-broker
replication, leader election, controller behavior, or Kafka wire
compatibility.

## Related Documentation

-   [System Architecture](architecture.md)
-   [Communication Protocol](protocol.md)
-   [Storage Engine](storage-engine.md)
-   [Producer](producer.md)
-   [Consumer](consumer.md)
-   [Consumer Groups](consumer-groups.md)

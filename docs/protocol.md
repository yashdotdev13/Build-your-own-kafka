# Communication Protocol

## Overview

Build-your-own-kafka uses a custom binary request/response protocol over
TCP to let clients communicate with the broker.

The protocol is specific to this project. It is inspired by the general
request/response approach used by message brokers, but it is not
compatible with the Apache Kafka wire protocol.

## TCP Framing

TCP provides a byte stream rather than application-level message
boundaries. The project uses framing so the receiver can determine where
one protocol message ends and the next begins.

The framing layer is responsible for reading and writing complete
messages over a persistent TCP connection. This allows multiple requests
and responses to travel over the same connection without treating each
socket read as a complete message.

## Request Structure

A request contains the information needed by the broker to identify and
process an operation:

-   **Request type** --- identifies the operation, such as `PING`,
    `PRODUCE`, `FETCH`, or `METADATA`.
-   **API version** --- a version field included in the request
    structure.
-   **Correlation ID** --- an identifier used to match a response with
    its request.
-   **Payload** --- operation-specific bytes encoded by the
    corresponding payload class.

The request type determines which handler the broker invokes. The
payload format depends on that request type.

## Response Structure

A response includes:

-   **Correlation ID** --- echoes the identifier from the request.
-   **Status** --- indicates whether the operation succeeded or returned
    an error.
-   **Payload** --- operation-specific response data or error details.

The client checks the response status and decodes the payload using the
response type associated with the operation.

## Request Dispatching

The broker's request dispatcher maps request type identifiers to request
handlers.

At a high level:

1.  A framed message is decoded into a request.
2.  The dispatcher reads the request type.
3.  The matching handler processes the request.
4.  The handler returns a response.
5.  The response is encoded and sent back over the connection.

This keeps connection-level message handling separate from the logic for
individual broker operations.

## Payload Encoding

Payload classes encode and decode operation-specific data. For example:

-   A produce payload identifies a topic and partition and carries a
    record value.
-   A batch produce payload identifies a topic and partition and carries
    multiple record values.
-   A batch produce response carries the base offset assigned to the
    batch and the number of records in it.

Payload encoding uses binary fields such as lengths, identifiers,
counts, and byte arrays. Decoders validate input and reject malformed
data such as invalid lengths or unexpected trailing bytes where
implemented.

## Produce Batch Example

A batch produce request contains multiple records for one topic
partition. The broker appends them in order and returns a response
containing:

-   **Base offset** --- the offset assigned to the first record in the
    batch.
-   **Record count** --- the number of records in the batch.

For a batch with base offset `10` and record count `3`, the records
occupy offsets `10`, `11`, and `12`, assuming the batch append completes
as expected.

## Error Handling

Handlers can return an error response when an operation cannot be
completed, such as when a referenced topic does not exist or the payload
is invalid.

The error payload may contain a human-readable message. Clients should
inspect the response status before attempting to decode a success
payload.

## Protocol Boundaries

The protocol layer defines message and payload formats. It does not own
topic storage, partition offset assignment, or consumer-group state.
Those responsibilities belong to broker components.

Similarly, the protocol does not itself provide replication, encryption,
authentication, or compatibility with Apache Kafka.

## Related Documentation

-   [System Architecture](architecture.md)
-   [Broker](broker.md)
-   [Producer](producer.md)
-   [Consumer](consumer.md)

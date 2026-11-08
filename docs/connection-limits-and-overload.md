# Connection Limits and Overload Handling

## Overview

The broker limits the number of concurrently active client connections
to protect the server from unbounded connection-handler growth.

Connection admission is controlled by a semaphore. Each admitted
connection holds one permit for the lifetime of its connection handler.

## Configuration

The default maximum number of concurrent client connections is **100**.

The default is defined by `BrokerNetworkConfig.MAX_CONNECTIONS`.

`BrokerServer` also supports a constructor that accepts a custom maximum
connection count. The configured value must be greater than zero;
otherwise, construction fails with `IllegalArgumentException`.

## Admission behavior

When a client connects, the broker attempts to acquire a connection
permit using `Semaphore.tryAcquire()`.

-   **Capacity available:** the broker acquires a permit and starts a
    virtual thread to handle the client connection.
-   **Capacity exhausted:** the broker does not wait for capacity. It
    logs a connection-rejection message and closes the newly accepted
    socket.

A rejected connection is not placed in an application-level waiting
queue. Clients that are rejected must retry according to their own retry
policy.

## Permit lifecycle

A permit is acquired before a client handler is started.

The handler releases the permit in its `finally` block, so the permit is
returned when the handler terminates, including when connection handling
ends with an I/O error.

If starting the virtual thread fails, the broker releases the acquired
permit and closes the client socket.

## Execution and resource bounds

The broker uses one virtual thread per admitted client connection. The
semaphore bounds the number of concurrently admitted connections and
therefore the number of active connection handlers started through this
admission path.

The broker does not maintain an application-level queue of excess
connections. Excess accepted sockets are closed immediately.

The operating system's TCP listen backlog is separate from this
application connection limit and is governed by socket and
operating-system behavior.

## Overload outcome

When the configured connection limit has been reached, a new client
connection is rejected by closing its socket. The broker logs:

> Connection rejected: broker connection limit reached.

The broker continues accepting connections and serving already-admitted
clients. Once an admitted connection terminates and its permit is
released, the broker can admit another client.

## Verification

`BrokerServerTest` uses real TCP sockets and a broker instance to verify
that:

-   connections beyond the configured limit are closed;
-   an admitted client can still send `PING` and receive `PONG` during
    overload;
-   repeated excess connection attempts do not prevent the admitted
    client from receiving a response;
-   a permit is released after a client disconnects, allowing a
    subsequent client to connect; and
-   zero and negative connection limits are rejected.

Run the focused tests with:

``` bash
mvn test -Dtest=BrokerServerTest
```

Run the complete test suite with:

``` bash
mvn clean test
```

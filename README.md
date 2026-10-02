<div align="center">

# 📨 Build Your Own Kafka

### A Kafka-inspired distributed messaging broker built from scratch with Java 21

Build the fundamentals behind a modern event-streaming platform — from raw TCP connections and binary framing to topics, partitions, records, and offsets.

<p>
  <img src="https://img.shields.io/badge/Java-21-orange?style=for-the-badge&logo=openjdk" alt="Java 21"/>
  <img src="https://img.shields.io/badge/Maven-3.x-C71A36?style=for-the-badge&logo=apachemaven" alt="Maven"/>
  <img src="https://img.shields.io/badge/TCP-Networking-005571?style=for-the-badge" alt="TCP"/>
  <img src="https://img.shields.io/badge/Protocol-Binary-6f42c1?style=for-the-badge" alt="Binary Protocol"/>
  <img src="https://img.shields.io/badge/Status-In%20Development-yellow?style=for-the-badge" alt="In Development"/>
</p>

<p>
  <a href="#-features">Features</a> •
  <a href="#-architecture">Architecture</a> •
  <a href="#-protocol">Protocol</a> •
  <a href="#-project-structure">Structure</a> •
  <a href="#-getting-started">Getting Started</a> •
  <a href="#-roadmap">Roadmap</a>
</p>

</div>

---

## 🧭 Overview

**Build Your Own Kafka** is a ground-up implementation of a Kafka-inspired message broker.

Instead of depending on Apache Kafka, this project intentionally starts at the lowest practical level and builds the broker layer by layer.

The project is designed as a systems-engineering learning project focused on understanding **how a log-based messaging system works internally**.

The implementation currently covers:

- TCP server/client communication and persistent connections
- Java 21 virtual threads
- Length-prefixed binary framing and a custom request/response protocol
- Protocol versioning, correlation IDs, request dispatching, and handler architecture
- Topics, dynamic partition counts, partitions, records, and partition-local offsets
- Single-record and batched message production
- Fetching records by offset, with a configurable batch limit
- Disk-backed partition logs, log segments, and recovery after restart
- Consumer offset storage and commit/fetch operations
- Consumer groups, membership, partition assignment, heartbeats, and rebalancing
- Metadata requests

The project has moved beyond the initial in-memory broker: it now includes production, fetching, persistent storage, and consumer-group functionality. Replication and multi-broker coordination remain future work.

---

## 📖 Detailed Documentation

The README keeps the project overview and development guide in one place. The following documents provide deeper explanations of individual areas:

- [System Architecture](docs/architecture.md) — component boundaries and end-to-end flows
- [Communication Protocol](docs/protocol.md) — framing, request/response structure, and payload encoding
- [Broker Internals](docs/broker.md) — request handling, topics, partitions, and broker operations
- [Storage Engine](docs/storage-engine.md) — log files, segments, append/read behavior, and recovery
- [Producer](docs/producer.md) — single-message and batch production flows

---

## 🎯 Project Vision

The long-term goal is to evolve this project from a simple TCP broker into a distributed, Kafka-inspired event streaming platform.

The target architecture is:

```text
                         ┌──────────────────┐
                         │     Producer     │
                         └────────┬─────────┘
                                  │
                                  │ TCP / Binary Protocol
                                  ▼
                         ┌──────────────────┐
                         │      Broker      │
                         └────────┬─────────┘
                                  │
                         ┌────────┴────────┐
                         │                 │
                         ▼                 ▼
                    Topic Manager      Request Layer
                         │
                         ▼
                       Topic
                         │
             ┌───────────┼───────────┐
             ▼           ▼           ▼
        Partition 0 Partition 1 Partition 2
             │           │           │
             ▼           ▼           ▼
           Log         Log         Log
             │
             ▼
          Records
             │
             ▼
          Offsets
             │
             ▼
                         Consumer
```

The next major areas to explore are:

- Multi-broker replication
- Leader/follower architecture
- Leader election
- Distributed coordination
- Replica recovery and fault tolerance
- Production hardening and performance testing

---

# ✨ Features

## 🌐 TCP Networking

The broker runs as a TCP server on port `9092`.

```text
Client
   │
   │ TCP connection
   ▼
BrokerServer
   │
   ▼
ClientConnection
```

Implemented:

- TCP server socket
- Client connections
- Persistent connections
- Multiple requests over a single connection
- Connection lifecycle handling
- Java 21 virtual threads

Each client connection is handled independently.

---

## 🧵 Java 21 Virtual Threads

Client connections are handled using Java 21 virtual threads:

```java
Thread.startVirtualThread(
    () -> handleClient(clientSocket)
);
```

This provides a lightweight concurrency model suitable for a broker that may eventually handle many simultaneous client connections.

---

# 📦 Binary Frame Protocol

TCP provides a continuous byte stream. It does **not** preserve application-level message boundaries.

Therefore, the broker implements its own framing layer.

### Current frame format

```text
┌──────────────────────┬─────────────────────────┐
│ Payload Length       │ Payload                 │
│ 4 bytes              │ N bytes                 │
└──────────────────────┴─────────────────────────┘
```

The decoder:

- Reads the length prefix
- Handles EOF
- Rejects negative lengths
- Rejects oversized frames
- Uses `readFully()` to guarantee complete payload reads

The current maximum frame size is **1 MB**.

---

# 🔄 Request / Response Protocol

On top of the frame layer, the broker implements a custom binary request/response protocol.

## Request

```text
┌───────────────────┐
│ Type              │ 4 bytes
├───────────────────┤
│ Version           │ 2 bytes
├───────────────────┤
│ Correlation ID    │ 4 bytes
├───────────────────┤
│ Payload Length    │ 4 bytes
├───────────────────┤
│ Payload           │ N bytes
└───────────────────┘
```

## Response

```text
┌───────────────────┐
│ Correlation ID    │ 4 bytes
├───────────────────┤
│ Status            │ 4 bytes
├───────────────────┤
│ Payload Length    │ 4 bytes
├───────────────────┤
│ Payload           │ N bytes
└───────────────────┘
```

This gives us a clear separation between:

```text
TCP Transport
      ↓
Frame
      ↓
Request / Response
      ↓
Broker Operation
```

---

# 🔢 Correlation IDs

Every request contains a correlation ID.

The broker copies the same ID into the response.

Example:

```text
Client
  │
  │ Request
  │ correlationId = 42
  ▼
Broker
  │
  │ Response
  │ correlationId = 42
  ▼
Client
```

This allows the client to associate a response with the request that generated it.

---

# 🧩 Request Dispatcher & Handler Architecture

The broker uses a handler-based request architecture.

```text
                    Request
                       │
                       ▼
              RequestDispatcher
                       │
              ┌────────┴────────┐
              │                 │
              ▼                 ▼
       PingRequestHandler   CreateTopicRequestHandler
```

The dispatcher maintains a mapping between request types and handlers.

The broker now supports request handlers for the implemented broker APIs, including `PING`, `CREATE_TOPIC`, `PRODUCE`, `PRODUCE_BATCH`, `FETCH`, metadata, and consumer-group operations. Request type identifiers are defined by the protocol implementation; see [Communication Protocol](docs/protocol.md) for the protocol details.

The `RequestHandler` abstraction keeps request routing separate from broker/domain logic.

---

# 🏓 PING API

The first implemented broker operation is `PING`.

```text
Client
  │
  │ PING
  ▼
Broker
  │
  │ PONG
  ▼
Client
```

Example response:

```text
PING RESPONSE
Correlation ID: 42
Status: 0
Payload: PONG
```

Where:

```text
SUCCESS = 0
ERROR   = 1
```

---

# 🗂️ Topics

The broker supports topic creation and management.

A topic is represented as:

```text
Topic
├── name
└── partitions
```

Topics are maintained by a shared `TopicManager`.

```text
BrokerServer
      │
      ▼
 TopicManager
      │
      ├── orders
      ├── payments
      └── ...
```

The manager uses `ConcurrentHashMap` for concurrent topic access.

---

# 🏗️ Dynamic Topic Partitions

Topics can be created with a configurable number of partitions.

Example:

```text
CREATE_TOPIC("orders", 3)
```

creates:

```text
orders
├── Partition 0
├── Partition 1
└── Partition 2
```

The topic validates:

- Non-empty topic names
- Positive partition counts

Partition IDs start at `0`.

---

# 📡 CREATE_TOPIC API

`CREATE_TOPIC` is currently implemented end-to-end over TCP.

### Request type

```text
CREATE_TOPIC = 2
```

### Payload format

```text
┌──────────────────────┐
│ Topic Name Length    │ 4 bytes
├──────────────────────┤
│ Topic Name           │ N bytes
├──────────────────────┤
│ Partition Count      │ 4 bytes
└──────────────────────┘
```

Example:

```text
Topic Name      = orders
Partition Count = 3
```

The payload is handled by:

```text
CreateTopicPayload
```

which is responsible for encoding and decoding the binary representation.

### End-to-end flow

```text
TestClient
    │
    │ CREATE_TOPIC("orders", 3)
    ▼
TCP
    │
    ▼
RequestDecoder
    │
    ▼
RequestDispatcher
    │
    ▼
CreateTopicRequestHandler
    │
    ▼
CreateTopicPayload.decode()
    │
    ├── orders
    └── 3
    │
    ▼
TopicManager
    │
    ▼
Topic
    │
    ├── Partition 0
    ├── Partition 1
    └── Partition 2
```

---

# 🧱 Partitions

Each topic owns a collection of partitions.

```text
orders
│
├── Partition 0
├── Partition 1
└── Partition 2
```

Each partition has:

- A partition ID
- An ordered record collection
- Sequential offsets
- Append support
- Single-record reads
- Offset-based reads

---

# 📝 Records

A record currently contains:

```text
Record
├── offset
└── value
```

Conceptually:

```text
Partition 0

Offset 0 → "Hello"
Offset 1 → "Kafka"
Offset 2 → "World"
```

The record value is stored as a byte array so that the storage model is not restricted to Java `String` values.

---

# 🔢 Partition-Local Offsets

Every partition maintains its own offset sequence.

Example:

```text
orders / Partition 0

Offset 0 → order-101
Offset 1 → order-102
Offset 2 → order-103
```

while:

```text
orders / Partition 1

Offset 0 → order-104
Offset 1 → order-105
```

Therefore, offsets are **local to a partition**.

A record is conceptually identified by:

```text
(topic, partition, offset)
```

The next offset is maintained as part of the partition log state and restored during recovery, rather than relying only on the current in-memory record count.

---

# 💾 Partition Log and Persistent Storage

The first version of each partition used an ordered in-memory collection. The current implementation has evolved to a disk-backed append-only log with log segments, while retaining partition-local offsets and offset-based reads.

Conceptually, the storage hierarchy is:

```text
Topic
  └── Partition
        └── Partition Log
              ├── segment files
              ├── records
              └── offsets
```

The storage layer supports appending records, reading records from an offset, segment rolling, and recovering log state when the broker starts again.

The key operations remain conceptually similar to the original in-memory model:

```java
append(byte[] value)
read(long offset)
readFrom(long offset)
nextOffset()
size()
```

For the implementation details, including the on-disk record layout and recovery behavior, see [Storage Engine](docs/storage-engine.md).

# 🔐 Concurrency

Partition append operations are currently synchronized.

The append operation must preserve the invariant:

```text
No two records in the same partition
may receive the same offset.
```

Conceptually:

```text
Producer A ─────┐
                ▼
            Partition 0
                ▲
Producer B ─────┘
```

The current implementation prioritizes correctness and simplicity. More advanced concurrency optimizations can be explored later.

---

# 🧪 Verified Tests

The implementation has been tested incrementally.

## Topic Test

Example:

```text
Created topic: orders
Orders partitions: 3
Created topic: payments
Payments partitions: 2
Topic count: 2
Orders partition 0: 0
Orders partition 1: 1
Orders partition 2: 2
```

## Partition Test

Example:

```text
First offset: 0
Second offset: 1
Third offset: 2
Next offset: 3
Partition size: 3
Record at offset 1: Kafka
Records from offset 1:
Offset 1 -> Kafka
Offset 2 -> World
```

## Network Test

Example:

```text
Connected to broker.

PING RESPONSE
Correlation ID: 42
Status: 0
Payload: PONG

Sending CREATE_TOPIC: orders, partitions=3

CREATE_TOPIC RESPONSE
Correlation ID: 100
Status: 0
Payload: orders
```

Broker:

```text
Received request: type=1, correlationId=42
Received request: type=2, correlationId=100
Topic created: orders with 3 partition(s)
```

---

# 🏛️ Architecture

The current system is intentionally separated into layers.

```text
┌───────────────────────────────────────────────────────────────┐
│                         Client Layer                          │
│                                                               │
│                         TestClient                            │
└───────────────────────────────┬───────────────────────────────┘
                                │
                                │ TCP
                                ▼
┌───────────────────────────────────────────────────────────────┐
│                       Transport Layer                         │
│                                                               │
│                     BrokerServer                              │
│                          │                                    │
│                   ClientConnection                            │
└───────────────────────────────┬───────────────────────────────┘
                                │
                                ▼
┌───────────────────────────────────────────────────────────────┐
│                       Protocol Layer                          │
│                                                               │
│ Frame → Request → Dispatcher → Response                       │
│                                                               │
│ FrameEncoder / FrameDecoder                                   │
│ RequestEncoder / RequestDecoder                               │
│ ResponseEncoder / ResponseDecoder                             │
└───────────────────────────────┬───────────────────────────────┘
                                │
                                ▼
┌───────────────────────────────────────────────────────────────┐
│                        Broker Layer                           │
│                                                               │
│ RequestHandler                                                 │
│ PingRequestHandler                                             │
│ CreateTopicRequestHandler                                     │
│ TopicManager                                                   │
└───────────────────────────────┬───────────────────────────────┘
                                │
                                ▼
┌───────────────────────────────────────────────────────────────┐
│                         Storage Model                         │
│                                                               │
│ Topic                                                          │
│   │                                                            │
│   ├── Partition                                                 │
│   │      └── Record                                             │
│   │             └── Offset                                     │
│   │                                                            │
│   └── Partition                                                 │
└───────────────────────────────────────────────────────────────┘
```

---

# 📁 Project Structure

```text
Build-your-own-kafka/
│
├── pom.xml
├── README.md
│
└── src/
    └── main/
        └── java/
            └── com/
                └── buildyourownkafka/
                    │
                    ├── Main.java
                    │
                    ├── broker/
                    │   ├── BrokerServer.java
                    │   ├── ClientConnection.java
                    │   ├── RequestDispatcher.java
                    │   ├── RequestHandler.java
                    │   ├── PingRequestHandler.java
                    │   ├── CreateTopicRequestHandler.java
                    │   ├── Topic.java
                    │   ├── TopicManager.java
                    │   ├── Partition.java
                    │   └── Record.java
                    │
                    ├── client/
                    │   ├── TestClient.java
                    │   ├── TopicTest.java
                    │   └── PartitionTest.java
                    │
                    └── protocol/
                        ├── Frame.java
                        ├── FrameEncoder.java
                        ├── FrameDecoder.java
                        ├── Request.java
                        ├── RequestEncoder.java
                        ├── RequestDecoder.java
                        ├── Response.java
                        ├── ResponseEncoder.java
                        ├── ResponseDecoder.java
                        └── CreateTopicPayload.java
```

---

# 🛠️ Tech Stack

| Technology | Purpose |
|---|---|
| **Java 21** | Core implementation |
| **Maven** | Build and dependency management |
| **TCP / Sockets** | Client-broker networking |
| **Binary Protocol** | Efficient request/response communication |
| **Virtual Threads** | Connection concurrency |
| **ConcurrentHashMap** | Thread-safe topic management |
| **Synchronized Partition Log** | Safe concurrent appends |

No Apache Kafka broker is used as the implementation underneath this project.

---

# 🚀 Getting Started

## Prerequisites

Make sure you have:

- Java 21+
- Maven 3.x+
- Git

Verify Java:

```bash
java -version
```

Verify Maven:

```bash
mvn -version
```

---

## Clone the Repository

```bash
git clone https://github.com/yashdotdev13/Build-your-own-kafka.git
cd Build-your-own-kafka
```

---

## Build

```bash
mvn clean package
```

Expected:

```text
BUILD SUCCESS
```

---

# ▶️ Run the Broker

Start the broker:

```bash
java -cp target/classes com.buildyourownkafka.Main
```

Expected:

```text
========================================
        Build Your Own Kafka
========================================

Broker starting...
Port: 9092

Broker started successfully.
Waiting for connections...
```

---

# 🧪 Run the Tests

## Topic Test

```bash
java -cp target/classes com.buildyourownkafka.client.TopicTest
```

## Partition Test

```bash
java -cp target/classes com.buildyourownkafka.client.PartitionTest
```

## Network Client

Start the broker first, then from another terminal:

```bash
java -cp target/classes com.buildyourownkafka.client.TestClient
```

---

# 📊 Current Progress

| Component | Status |
|---|:---:|
| Java 21 setup and Maven build | ✅ |
| TCP server/client and persistent connections | ✅ |
| Virtual-thread connection handling | ✅ |
| Length-prefixed framing | ✅ |
| Request/response protocol, version, correlation IDs | ✅ |
| Request dispatcher and handler architecture | ✅ |
| PING API | ✅ |
| Topic management and dynamic partitions | ✅ |
| Partition records and sequential offsets | ✅ |
| Single-record PRODUCE API | ✅ |
| Batched PRODUCE API | ✅ |
| FETCH API and fetch batch limit | ✅ |
| Disk persistence and log segments | ✅ |
| Recovery after restart | ✅ |
| Consumer offset storage and commit/fetch | ✅ |
| Consumer groups and partition assignment | ✅ |
| Heartbeats, failure detection, and rebalancing | ✅ |
| Metadata API | ✅ |
| Replication | ⬜ |
| Leader/follower model | ⬜ |
| Leader election | ⬜ |
| Multi-broker fault tolerance | ⬜ |

Legend:

```text
✅ Completed
🚧 In Progress
⬜ Planned
```

# 🗺️ Roadmap

The project will evolve in stages.

## Phase 1 — Networking Foundation

- [x] TCP server
- [x] TCP client
- [x] Persistent connections
- [x] Virtual threads
- [x] Binary framing
- [x] Request/response protocol

## Phase 2 — Broker Protocol

- [x] Protocol version
- [x] Correlation IDs
- [x] Request dispatcher
- [x] Handler architecture
- [x] PING API
- [x] CREATE_TOPIC API

## Phase 3 — Topic & Partition Model

- [x] Topic
- [x] TopicManager
- [x] Dynamic partition count
- [x] Partition
- [x] Record
- [x] Sequential offsets
- [x] In-memory partition log

## Phase 4 — Producer

- [x] PRODUCE request
- [x] Produce payload
- [x] Topic and partition lookup
- [x] Record append
- [x] Offset response
- [x] Produce error handling
- [x] Batched production

Implemented flow:

```text
Producer
   │
   │ PRODUCE / PRODUCE_BATCH
   ▼
Broker
   │
   ▼
Topic and Partition
   │
   ▼
Append Record(s)
   │
   ▼
Return base offset / result
```

## Phase 5 — Consumer and Fetching

- [x] FETCH request
- [x] Fetch payload
- [x] Read from offset
- [x] Return records
- [x] Fetch batch limit
- [x] Consumer-side network APIs

Implemented flow:

```text
Consumer
   │
   │ FETCH(offset)
   ▼
Broker
   │
   ▼
Partition Log
   │
   ▼
Records from requested offset
```

## Phase 6 — Persistent Storage

- [x] Append-only log files
- [x] File-backed records
- [x] Log segments
- [x] Segment rolling
- [x] Recovery after restart
- [x] Offset continuity across recovery

The original in-memory log was replaced by a disk-backed partition log. See [Storage Engine](docs/storage-engine.md) for the storage design and behavior.

## Phase 7 — Consumer Groups

- [x] Consumer registration and group membership
- [x] Partition assignment
- [x] Offset tracking and commits
- [x] Heartbeats
- [x] Failure detection
- [x] Rebalancing
- [x] Generation validation and rejoin behavior

Consumer-group coordination is implemented within the current single-broker scope. Distributed coordination across brokers remains future work.

## Phase 8 — Replication

- [ ] Broker identity
- [ ] Leader partition
- [ ] Followers
- [ ] Replication protocol
- [ ] Replication offsets
- [ ] High watermark concepts

## Phase 9 — Leader Election

- [ ] Broker failure detection
- [ ] Leader election
- [ ] New leader selection
- [ ] Replica recovery

## Phase 10 — Distributed Broker

Eventually:

```text
                    ┌───────────────┐
                    │    Client     │
                    └───────┬───────┘
                            │
                            ▼
                 ┌────────────────────┐
                 │      Broker 1       │
                 │     Leader          │
                 └─────────┬──────────┘
                           │
                  replication
                    ┌──────┴──────┐
                    ▼             ▼
             ┌───────────┐ ┌───────────┐
             │  Broker 2 │ │  Broker 3 │
             │  Follower │ │  Follower │
             └───────────┘ └───────────┘
```

---

# 🔬 Engineering Principles

This project intentionally follows several principles.

### 1. Build from the bottom up

```text
TCP
 ↓
Framing
 ↓
Protocol
 ↓
Broker
 ↓
Storage
 ↓
Producer
 ↓
Consumer
 ↓
Replication
```

### 2. Separate concerns

Transport, protocol, request handling, broker state, and storage are kept separate.

### 3. Validate each layer independently

For example:

```text
TopicTest
PartitionTest
TestClient
```

are used to verify individual layers before adding more complexity.

### 4. Prefer correctness before optimization

The first implementation should be easy to reason about.

Performance optimizations can be introduced after the behavior and invariants are correct.

### 5. Understand the system instead of hiding complexity

Where possible, functionality is implemented manually rather than delegated to high-level messaging frameworks.

---

# 📚 What This Project Teaches

Building this project provides hands-on experience with:

- Network programming
- TCP streams
- Binary serialization
- Protocol design
- Backward-compatible protocol evolution
- Request dispatching
- Concurrency
- Virtual threads
- Thread safety
- Distributed-system fundamentals
- Append-only logs
- Partitioning
- Offsets
- Producer/consumer architecture
- Persistent storage
- Replication
- Leader election
- Fault tolerance

---

# ⚠️ Current Limitations

The implementation has progressed beyond the initial broker, but it is still a learning project and is not a drop-in replacement for Apache Kafka.

### Single broker

There is currently no multi-broker replication or distributed coordination.

### No replication or leader election

Partition replicas, leader/follower behavior, leader election, and replica recovery remain planned.

### Simplified protocol and client model

The project uses its own Kafka-inspired protocol and client implementation. It does not implement the Apache Kafka wire protocol or guarantee compatibility with Kafka clients.

### Production hardening remains future work

Operational concerns such as security, authentication, production-grade configuration, extensive load testing, and comprehensive failure handling are outside the current scope.

These limitations are intentional because the project is being built incrementally.

# 🤝 Contributing

This is primarily a learning and systems-engineering project, but improvements, discussions, and experiments are welcome.

Suggested workflow:

```text
Create branch
     ↓
Implement one concept
     ↓
Add/update test
     ↓
Run mvn clean package
     ↓
Verify behavior
     ↓
Commit
```

Keep changes focused around a single broker capability.

---

# 📌 Project Philosophy

> Don't just use distributed systems. Build one.

The purpose of this project is to move beyond using Kafka as a black box and understand the engineering ideas that make a Kafka-like system possible.

Every layer is implemented deliberately:

```text
"How does TCP deliver bytes?"
          ↓
"How do we identify a message?"
          ↓
"How do we encode a request?"
          ↓
"How does the broker route it?"
          ↓
"How do topics work?"
          ↓
"Why do we need partitions?"
          ↓
"How are records ordered?"
          ↓
"Why do offsets exist?"
          ↓
"How do producers and consumers interact?"
          ↓
"How does the system survive failures?"
```

That is the journey this repository is documenting.

---

<div align="center">

## 🚀 Build. Break. Understand. Rebuild.

**Build Your Own Kafka — one distributed-system concept at a time.**

</div>

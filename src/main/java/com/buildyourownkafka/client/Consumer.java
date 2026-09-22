package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.HeartbeatRequestPayload;
import com.buildyourownkafka.broker.HeartbeatResponsePayload;
import com.buildyourownkafka.broker.JoinGroupRequestPayload;
import com.buildyourownkafka.broker.JoinGroupResponsePayload;
import com.buildyourownkafka.broker.Record;
import com.buildyourownkafka.broker.SyncGroupRequestPayload;
import com.buildyourownkafka.broker.SyncGroupResponsePayload;
import com.buildyourownkafka.protocol.CommitOffsetPayload;
import com.buildyourownkafka.protocol.FetchOffsetPayload;
import com.buildyourownkafka.protocol.FetchOffsetResponsePayload;
import com.buildyourownkafka.protocol.FetchPayload;
import com.buildyourownkafka.protocol.FetchResponsePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestEncoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseDecoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.UUID;

public class Consumer implements AutoCloseable {

    private final String topic;
    private final int partition;
    private final String consumerGroupId;

    private long currentOffset;
    private String memberId;
    private int generation;
    private List<Integer> assignedPartitions;

    private final Socket socket;
    private final DataInputStream input;
    private final DataOutputStream output;

    private final RequestEncoder requestEncoder;
    private final ResponseDecoder responseDecoder;

    /*
     * Protects the complete request/response cycle.
     *
     * A request must be written and its corresponding response
     * must be read before another request can use the socket.
     */
    private final Object requestLock = new Object();

    private int correlationId = 1000;

    private static final long DEFAULT_HEARTBEAT_INTERVAL_MILLIS = 3000L;

    private static final int DEFAULT_FETCH_MAX_RECORDS = 10;

    private volatile boolean heartbeatRunning;
    private Thread heartbeatThread;

    public Consumer(
            String host,
            int port,
            String topic,
            int partition,
            long startingOffset
    ) throws Exception {

        this(
                host,
                port,
                topic,
                partition,
                startingOffset,
                null
        );
    }

    public Consumer(
            String host,
            int port,
            String topic,
            int partition,
            long startingOffset,
            String consumerGroupId
    ) throws Exception {

        validateHost(host);
        validatePort(port);
        validateTopic(topic);
        validatePartition(partition);
        validateOffset(startingOffset);

        if (consumerGroupId != null && consumerGroupId.isBlank()) {
            throw new IllegalArgumentException(
                    "Consumer group ID cannot be blank"
            );
        }

        this.topic = topic;
        this.partition = partition;
        this.currentOffset = startingOffset;
        this.consumerGroupId = consumerGroupId;
        this.memberId = "consumer-" + UUID.randomUUID();
        this.generation = 0;
        this.assignedPartitions = List.of();

        this.socket = new Socket(host, port);
        this.input = new DataInputStream(
                socket.getInputStream()
        );
        this.output = new DataOutputStream(
                socket.getOutputStream()
        );

        this.requestEncoder = new RequestEncoder(output);
        this.responseDecoder = new ResponseDecoder(input);
    }

    public Consumer(
            String host,
            int port,
            String topic,
            int partition,
            String consumerGroupId
    ) throws Exception {

        this(
                host,
                port,
                topic,
                partition,
                0L,
                consumerGroupId
        );

        this.currentOffset = fetchCommittedOffset();
    }

    public void joinGroup(int partitionCount) throws Exception {

        synchronized (requestLock) {

            if (consumerGroupId == null) {
                throw new IllegalStateException(
                        "Consumer group ID is required to join a group"
                );
            }

            if (partitionCount <= 0) {
                throw new IllegalArgumentException(
                        "Partition count must be greater than zero"
                );
            }

            JoinGroupRequestPayload payload =
                    new JoinGroupRequestPayload(
                            consumerGroupId,
                            memberId,
                            partitionCount
                    );

            Request request =
                    new Request(
                            Request.JOIN_GROUP,
                            (short) 1,
                            nextCorrelationId(),
                            payload.encode()
                    );

            requestEncoder.encode(request);

            Response response =
                    responseDecoder.decode();

            if (response.status() != Response.SUCCESS) {
                throw new RuntimeException(
                        "Join group failed: "
                                + new String(response.payload())
                );
            }

            JoinGroupResponsePayload responsePayload =
                    JoinGroupResponsePayload.decode(
                            response.payload()
                    );

            synchronized (this) {
                this.memberId = responsePayload.memberId();
                this.generation = responsePayload.generation();
                this.assignedPartitions = List.of();
            }
        }
    }

    public void syncGroup() throws Exception {

        synchronized (requestLock) {

            String currentMemberId;
            int currentGeneration;

            synchronized (this) {
                currentMemberId = this.memberId;
                currentGeneration = this.generation;
            }

            if (consumerGroupId == null) {
                throw new IllegalStateException(
                        "Consumer group ID is required to sync group"
                );
            }

            if (currentMemberId == null
                    || currentMemberId.isBlank()) {

                throw new IllegalStateException(
                        "Consumer must join the group before syncing"
                );
            }

            if (currentGeneration < 0) {
                throw new IllegalStateException(
                        "Consumer generation cannot be negative"
                );
            }

            SyncGroupRequestPayload payload =
                    new SyncGroupRequestPayload(
                            consumerGroupId,
                            currentMemberId,
                            currentGeneration
                    );

            Request request =
                    new Request(
                            Request.SYNC_GROUP,
                            (short) 1,
                            nextCorrelationId(),
                            payload.encode()
                    );

            requestEncoder.encode(request);

            Response response =
                    responseDecoder.decode();

            if (response.status() != Response.SUCCESS) {
                throw new RuntimeException(
                        "Sync group failed: "
                                + new String(response.payload())
                );
            }

            SyncGroupResponsePayload responsePayload =
                    SyncGroupResponsePayload.decode(
                            response.payload()
                    );

            if (!currentMemberId.equals(
                    responsePayload.memberId())) {

                throw new IllegalStateException(
                        "SYNC_GROUP returned a different member ID"
                );
            }

            synchronized (this) {
                this.generation =
                        responsePayload.generation();

                this.assignedPartitions =
                        responsePayload.partitions();
            }

            startHeartbeat();
        }
    }

    public synchronized void startHeartbeat() {
        startHeartbeat(
                DEFAULT_HEARTBEAT_INTERVAL_MILLIS
        );
    }

    public synchronized void startHeartbeat(
            long heartbeatIntervalMillis
    ) {

        if (consumerGroupId == null) {
            throw new IllegalStateException(
                    "Consumer group ID is required for heartbeat"
            );
        }

        if (heartbeatIntervalMillis <= 0) {
            throw new IllegalArgumentException(
                    "Heartbeat interval must be greater than zero"
            );
        }

        if (heartbeatRunning) {
            return;
        }

        heartbeatRunning = true;

        heartbeatThread =
                Thread.startVirtualThread(
                        () -> heartbeatLoop(
                                heartbeatIntervalMillis
                        )
                );

        System.out.println(
                "Consumer heartbeat started. Interval: "
                        + heartbeatIntervalMillis
                        + " ms"
        );
    }

    private void heartbeatLoop(
            long heartbeatIntervalMillis
    ) {

        while (
                heartbeatRunning
                        && !Thread.currentThread().isInterrupted()
        ) {

            try {

                heartbeat();

                System.out.println(
                        "Consumer heartbeat sent. "
                                + "member="
                                + memberId()
                                + ", generation="
                                + generation()
                );

            } catch (Exception e) {

                if (heartbeatRunning) {
                    System.err.println(
                            "Consumer heartbeat failed: "
                                    + e.getMessage()
                    );
                }

                break;
            }

            try {

                Thread.sleep(heartbeatIntervalMillis);

            } catch (InterruptedException e) {

                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public void heartbeat() throws Exception {

        synchronized (requestLock) {

            String currentMemberId;
            int currentGeneration;

            synchronized (this) {
                currentMemberId = this.memberId;
                currentGeneration = this.generation;
            }

            if (consumerGroupId == null) {
                throw new IllegalStateException(
                        "Consumer group ID is required to send heartbeat"
                );
            }

            if (currentMemberId == null
                    || currentMemberId.isBlank()) {

                throw new IllegalStateException(
                        "Consumer must join the group before sending heartbeat"
                );
            }

            if (currentGeneration < 0) {
                throw new IllegalStateException(
                        "Consumer generation cannot be negative"
                );
            }

            HeartbeatRequestPayload payload =
                    new HeartbeatRequestPayload(
                            consumerGroupId,
                            currentMemberId,
                            currentGeneration
                    );

            Request request =
                    new Request(
                            Request.HEARTBEAT,
                            (short) 1,
                            nextCorrelationId(),
                            payload.encode()
                    );

            requestEncoder.encode(request);

            Response response =
                    responseDecoder.decode();

            if (response.status() != Response.SUCCESS) {
                throw new RuntimeException(
                        "Heartbeat failed: "
                                + new String(response.payload())
                );
            }

            HeartbeatResponsePayload responsePayload =
                    HeartbeatResponsePayload.decode(
                            response.payload()
                    );

            if (!currentMemberId.equals(
                    responsePayload.memberId())) {

                throw new IllegalStateException(
                        "HEARTBEAT returned a different member ID"
                );
            }

            if (currentGeneration
                    != responsePayload.generation()) {

                throw new IllegalStateException(
                        "HEARTBEAT returned a different generation"
                );
            }
        }
    }

    public synchronized void stopHeartbeat() {

        if (!heartbeatRunning) {
            return;
        }

        heartbeatRunning = false;

        Thread thread = heartbeatThread;

        if (thread != null) {
            thread.interrupt();
        }

        heartbeatThread = null;

        System.out.println(
                "Consumer heartbeat stopped."
        );
    }

    public List<Record> poll() throws Exception {

        synchronized (requestLock) {

            long offset;

            synchronized (this) {
                offset = currentOffset;
            }

            FetchPayload payload =
                    new FetchPayload(
                            topic,
                            partition,
                            offset,
                            DEFAULT_FETCH_MAX_RECORDS
                    );

            Request request =
                    new Request(
                            Request.FETCH,
                            (short) 1,
                            nextCorrelationId(),
                            payload.encode()
                    );

            requestEncoder.encode(request);

            Response response =
                    responseDecoder.decode();

            if (response.status() != Response.SUCCESS) {
                throw new RuntimeException(
                        "Fetch failed: "
                                + new String(response.payload())
                );
            }

            FetchResponsePayload fetchResponse =
                    FetchResponsePayload.decode(
                            response.payload()
                    );

            List<Record> records =
                    fetchResponse.records();

            if (!records.isEmpty()) {

                Record lastRecord =
                        records.get(records.size() - 1);

                synchronized (this) {
                    currentOffset =
                            lastRecord.offset() + 1;
                }
            }

            return records;
        }
    }

    public void commit() throws Exception {

        synchronized (requestLock) {

            if (consumerGroupId == null) {
                throw new IllegalStateException(
                        "Consumer group ID is required to commit offsets"
                );
            }

            long offset;

            synchronized (this) {
                offset = currentOffset;
            }

            CommitOffsetPayload payload =
                    new CommitOffsetPayload(
                            consumerGroupId,
                            topic,
                            partition,
                            offset
                    );

            Request request =
                    new Request(
                            Request.COMMIT_OFFSET,
                            (short) 1,
                            nextCorrelationId(),
                            payload.encode()
                    );

            requestEncoder.encode(request);

            Response response =
                    responseDecoder.decode();

            if (response.status() != Response.SUCCESS) {
                throw new RuntimeException(
                        "Offset commit failed: "
                                + new String(response.payload())
                );
            }
        }
    }

    public void rejoinGroup(int partitionCount)
            throws Exception {

        if (consumerGroupId == null) {
            throw new IllegalStateException(
                    "Consumer group ID is required to rejoin a group"
            );
        }

        joinGroup(partitionCount);
        syncGroup();
    }

    private long fetchCommittedOffset()
            throws Exception {

        synchronized (requestLock) {

            if (consumerGroupId == null) {
                throw new IllegalStateException(
                        "Consumer group ID is required to fetch committed offset"
                );
            }

            FetchOffsetPayload payload =
                    new FetchOffsetPayload(
                            consumerGroupId,
                            topic,
                            partition
                    );

            Request request =
                    new Request(
                            Request.FETCH_OFFSET,
                            (short) 1,
                            nextCorrelationId(),
                            payload.encode()
                    );

            requestEncoder.encode(request);

            Response response =
                    responseDecoder.decode();

            if (response.status() != Response.SUCCESS) {
                throw new RuntimeException(
                        "Failed to fetch committed offset: "
                                + new String(response.payload())
                );
            }

            FetchOffsetResponsePayload responsePayload =
                    FetchOffsetResponsePayload.decode(
                            response.payload()
                    );

            return responsePayload.offset();
        }
    }

    private synchronized int nextCorrelationId() {
        return correlationId++;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public String consumerGroupId() {
        return consumerGroupId;
    }

    public synchronized long currentOffset() {
        return currentOffset;
    }

    public synchronized String memberId() {
        return memberId;
    }

    public synchronized int generation() {
        return generation;
    }

    public synchronized List<Integer> assignedPartitions() {
        return List.copyOf(assignedPartitions);
    }

    public synchronized boolean isGroupMember() {
        return memberId != null;
    }

    public synchronized void advanceOffset(long nextOffset) {

        if (nextOffset < currentOffset) {
            throw new IllegalArgumentException(
                    "Consumer offset cannot move backwards"
            );
        }

        this.currentOffset = nextOffset;
    }

    @Override
    public void close() throws Exception {

        stopHeartbeat();

        socket.close();

        System.out.println(
                "Consumer connection closed."
        );
    }

    private static void validateHost(String host) {

        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException(
                    "Host cannot be blank"
            );
        }
    }

    private static void validatePort(int port) {

        if (port <= 0) {
            throw new IllegalArgumentException(
                    "Port must be greater than zero"
            );
        }
    }

    private static void validateTopic(String topic) {

        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException(
                    "Topic cannot be blank"
            );
        }
    }

    private static void validatePartition(int partition) {

        if (partition < 0) {
            throw new IllegalArgumentException(
                    "Partition cannot be negative"
            );
        }
    }

    private static void validateOffset(long offset) {

        if (offset < 0) {
            throw new IllegalArgumentException(
                    "Starting offset cannot be negative"
            );
        }
    }
}
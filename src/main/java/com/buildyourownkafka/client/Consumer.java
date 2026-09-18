package com.buildyourownkafka.client;

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
    private int correlationId = 1000;

    public Consumer(String host, int port, String topic, int partition, long startingOffset) throws Exception {
        this(host, port, topic, partition, startingOffset, null);
    }
    public Consumer(String host, int port, String topic, int partition, long startingOffset, String consumerGroupId) throws Exception {

        validateHost(host);
        validatePort(port);
        validateTopic(topic);
        validatePartition(partition);
        validateOffset(startingOffset);

        if (consumerGroupId != null && consumerGroupId.isBlank()) {
            throw new IllegalArgumentException("Consumer group ID cannot be blank");
        }

        this.topic = topic;
        this.partition = partition;
        this.currentOffset = startingOffset;
        this.consumerGroupId = consumerGroupId;
        this.memberId = "consumer-" + UUID.randomUUID();
        this.generation = 0;
        this.assignedPartitions = List.of();
        this.socket = new Socket(host, port);
        this.input = new DataInputStream(socket.getInputStream());
        this.output = new DataOutputStream(socket.getOutputStream());
        this.requestEncoder = new RequestEncoder(output);
        this.responseDecoder = new ResponseDecoder(input);
    }
    public Consumer(String host, int port, String topic, int partition, String consumerGroupId) throws Exception {
        this(host, port, topic, partition, 0L, consumerGroupId);
        this.currentOffset = fetchCommittedOffset();
    }

    public synchronized void joinGroup(int partitionCount) throws Exception {

        if (consumerGroupId == null) {
            throw new IllegalStateException("Consumer group ID is required to join a group");
        }
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("Partition count must be greater than zero");
        }
        JoinGroupRequestPayload payload = new JoinGroupRequestPayload(consumerGroupId, memberId, partitionCount);
        Request request = new Request(Request.JOIN_GROUP, (short) 1, correlationId++, payload.encode());
        requestEncoder.encode(request);
        Response response = responseDecoder.decode();

        if (response.status() != Response.SUCCESS) {
            throw new RuntimeException("Join group failed: " + new String(response.payload()));
        }

        JoinGroupResponsePayload responsePayload = JoinGroupResponsePayload.decode(response.payload());
        this.memberId = responsePayload.memberId();
        this.generation = responsePayload.generation();
        this.assignedPartitions = List.of();
    }

    public synchronized void syncGroup() throws Exception {
        if (consumerGroupId == null) {
            throw new IllegalStateException("Consumer group ID is required to sync group");
        }
        if (memberId == null || memberId.isBlank()) {
            throw new IllegalStateException("Consumer must join the group before syncing");
        }
        if (generation < 0) {
            throw new IllegalStateException("Consumer generation cannot be negative");
        }
        SyncGroupRequestPayload payload = new SyncGroupRequestPayload(consumerGroupId, memberId, generation);
        Request request = new Request(Request.SYNC_GROUP, (short) 1, correlationId++, payload.encode());
        requestEncoder.encode(request);
        Response response = responseDecoder.decode();
        if (response.status() != Response.SUCCESS) {
            throw new RuntimeException("Sync group failed: " + new String(response.payload()));
        }
        SyncGroupResponsePayload responsePayload = SyncGroupResponsePayload.decode(response.payload());
        if (!memberId.equals(responsePayload.memberId())) {
            throw new IllegalStateException("SYNC_GROUP returned a different member ID");
        }

        this.generation = responsePayload.generation();
        this.assignedPartitions = responsePayload.partitions();
    }
    public synchronized List<Record> poll() throws Exception {

        FetchPayload payload = new FetchPayload(topic, partition, currentOffset);
        Request request = new Request(Request.FETCH, (short) 1, correlationId++, payload.encode());
        requestEncoder.encode(request);
        Response response = responseDecoder.decode();
        if (response.status() != Response.SUCCESS) {
            throw new RuntimeException("Fetch failed: " + new String(response.payload()));
        }
        FetchResponsePayload fetchResponse = FetchResponsePayload.decode(response.payload());
        List<Record> records = fetchResponse.records();
        if (!records.isEmpty()) {
            Record lastRecord = records.get(records.size() - 1);
            currentOffset = lastRecord.offset() + 1;
        }
        return records;
    }

    public synchronized void commit() throws Exception {
        if (consumerGroupId == null) {
            throw new IllegalStateException("Consumer group ID is required to commit offsets");
        }
        CommitOffsetPayload payload = new CommitOffsetPayload(consumerGroupId, topic, partition, currentOffset);
        Request request = new Request(Request.COMMIT_OFFSET, (short) 1, correlationId++, payload.encode());
        requestEncoder.encode(request);
        Response response = responseDecoder.decode();
        if (response.status() != Response.SUCCESS) {
            throw new RuntimeException("Offset commit failed: " + new String(response.payload()));
        }
    }
    public synchronized void rejoinGroup(int partitionCount) throws Exception {
        if (consumerGroupId == null) {
            throw new IllegalStateException("Consumer group ID is required to rejoin a group");
        }
        joinGroup(partitionCount);
        syncGroup();
    }
    private synchronized long fetchCommittedOffset() throws Exception {

        if (consumerGroupId == null) {
            throw new IllegalStateException("Consumer group ID is required to fetch committed offset");
        }
        FetchOffsetPayload payload = new FetchOffsetPayload(consumerGroupId, topic, partition);
        Request request = new Request(Request.FETCH_OFFSET, (short) 1, correlationId++, payload.encode());
        requestEncoder.encode(request);
        Response response = responseDecoder.decode();
        if (response.status() != Response.SUCCESS) {
            throw new RuntimeException("Failed to fetch committed offset: " + new String(response.payload()));
        }
        FetchOffsetResponsePayload responsePayload = FetchOffsetResponsePayload.decode(response.payload());
        return responsePayload.offset();
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
            throw new IllegalArgumentException("Consumer offset cannot move backwards");
        }
        this.currentOffset = nextOffset;
    }

    @Override
    public void close() throws Exception {
        socket.close();
    }
    private static void validateHost(String host) {

        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Host cannot be blank");
        }
    }
    private static void validatePort(int port) {
        if (port <= 0) {
            throw new IllegalArgumentException("Port must be greater than zero");
        }
    }
    private static void validateTopic(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic cannot be blank");
        }
    }
    private static void validatePartition(int partition) {
        if (partition < 0) {
            throw new IllegalArgumentException("Partition cannot be negative");
        }
    }
    private static void validateOffset(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Starting offset cannot be negative");
        }
    }
}
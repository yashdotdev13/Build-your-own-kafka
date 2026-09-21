package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroupCoordinator;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.HeartbeatRequestPayload;
import com.buildyourownkafka.broker.HeartbeatResponsePayload;
import com.buildyourownkafka.broker.RequestDispatcher;
import com.buildyourownkafka.broker.TopicManager;
import com.buildyourownkafka.broker.ConsumerOffsetStore;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.nio.file.Files;
import java.nio.file.Path;

public class HeartbeatDispatcherTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== HEARTBEAT DISPATCHER TEST ===");
        Path tempDirectory = Files.createTempDirectory("heartbeat-dispatcher-test");
        Path topicStorage = tempDirectory.resolve("topics");
        Path offsetStorage = tempDirectory.resolve("offsets");

        Files.createDirectories(topicStorage);
        Files.createDirectories(offsetStorage);

        TopicManager topicManager = new TopicManager(topicStorage);

        ConsumerOffsetStore consumerOffsetStore = new ConsumerOffsetStore(offsetStorage);
        ConsumerGroupManager groupManager = new ConsumerGroupManager();
        ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator(groupManager);
        RequestDispatcher dispatcher = new RequestDispatcher(topicManager, consumerOffsetStore, coordinator);

        String groupId = "orders-group";
        String memberId = "consumer-A";

        groupManager.getOrCreateGroup(groupId);
        groupManager.addMember(groupId, memberId);
        HeartbeatRequestPayload payload = new HeartbeatRequestPayload(groupId, memberId, 0);
        Request request = new Request(Request.HEARTBEAT, (short) 1, 200, payload.encode());
        Response response = dispatcher.dispatch(request);
        System.out.println("Response correlation ID: " + response.correlationId());
        System.out.println("Response status: " + response.status());
        if (response.status() != Response.SUCCESS) {
            throw new AssertionError("HEARTBEAT dispatch failed: " + new String(response.payload()));
        }
        if (response.correlationId() != 200) {
            throw new AssertionError("Correlation ID mismatch");
        }
        HeartbeatResponsePayload responsePayload = HeartbeatResponsePayload.decode(response.payload());

        System.out.println("Response member: " + responsePayload.memberId());
        System.out.println("Response generation: " + responsePayload.generation());
        if (!memberId.equals(responsePayload.memberId())) {
            throw new AssertionError("Member ID mismatch");
        }
        if (responsePayload.generation() != 0) {
            throw new AssertionError("Generation mismatch");
        }
        System.out.println("HEARTBEAT DISPATCHER VERIFIED!");
    }
}
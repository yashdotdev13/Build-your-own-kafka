package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroupCoordinator;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.ConsumerOffsetStore;
import com.buildyourownkafka.broker.RequestDispatcher;
import com.buildyourownkafka.broker.SyncGroupRequestPayload;
import com.buildyourownkafka.broker.SyncGroupResponsePayload;
import com.buildyourownkafka.broker.TopicManager;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.nio.file.Path;

public class SyncGroupDispatcherTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== SYNC GROUP DISPATCHER TEST ===");
        TopicManager topicManager = new TopicManager(Path.of("data", "test-topics"));
        ConsumerOffsetStore consumerOffsetStore = new ConsumerOffsetStore(Path.of("data", "test-offsets"));
        ConsumerGroupManager groupManager = new ConsumerGroupManager();
        ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator(groupManager);
        coordinator.joinGroup("orders-group", "consumer-A", 4);
        coordinator.joinGroup("orders-group", "consumer-B", 4);
        RequestDispatcher dispatcher = new RequestDispatcher(topicManager, consumerOffsetStore, coordinator);
        SyncGroupRequestPayload payload = new SyncGroupRequestPayload("orders-group", "consumer-A", 0);

        Request request = new Request(Request.SYNC_GROUP, (short) 1, 200, payload.encode());
        Response response = dispatcher.dispatch(request);

        if (response.status() != Response.SUCCESS) {
            throw new AssertionError("SYNC_GROUP dispatch failed");
        }
        if (response.correlationId() != 200) {
            throw new AssertionError("Correlation ID mismatch");
        }
        SyncGroupResponsePayload responsePayload = SyncGroupResponsePayload.decode(response.payload());
        System.out.println("Member ID: " + responsePayload.memberId());
        System.out.println("Generation: " + responsePayload.generation());
        System.out.println("Partitions: " + responsePayload.partitions());
        if (!responsePayload.memberId().equals("consumer-A")) {
            throw new AssertionError("Member ID mismatch");
        }
        if (responsePayload.generation() != 0) {
            throw new AssertionError("Generation mismatch");
        }
        if (!responsePayload.partitions().equals(java.util.List.of(0, 2))) {
            throw new AssertionError("Current assignment mismatch");
        }
        System.out.println();
        System.out.println("SYNC_GROUP dispatcher " + "verified successfully!");
    }
}
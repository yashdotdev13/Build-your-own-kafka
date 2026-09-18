package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroupCoordinator;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.JoinGroupRequestHandler;
import com.buildyourownkafka.broker.JoinGroupRequestPayload;
import com.buildyourownkafka.broker.SyncGroupRequestHandler;
import com.buildyourownkafka.broker.SyncGroupRequestPayload;
import com.buildyourownkafka.broker.SyncGroupResponsePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

public class SyncGroupGenerationValidationTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== SYNC GROUP GENERATION VALIDATION TEST ===");
        ConsumerGroupManager groupManager = new ConsumerGroupManager();
        ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator(groupManager);
        JoinGroupRequestHandler joinHandler = new JoinGroupRequestHandler(coordinator);
        JoinGroupRequestPayload joinA = new JoinGroupRequestPayload("orders-group", "consumer-A", 4);
        Response joinResponseA = joinHandler.handle(new Request(Request.JOIN_GROUP, (short) 1, 1, joinA.encode()));
        if (joinResponseA.status() != Response.SUCCESS) {
            throw new AssertionError("Consumer-A JOIN_GROUP failed");
        }
        JoinGroupRequestPayload joinB = new JoinGroupRequestPayload("orders-group", "consumer-B", 4);
        Response joinResponseB = joinHandler.handle(new Request(Request.JOIN_GROUP, (short) 1, 2, joinB.encode()));
        if (joinResponseB.status() != Response.SUCCESS) {
            throw new AssertionError("Consumer-B JOIN_GROUP failed");
        }

        int currentGeneration = groupManager.getGroup("orders-group").generation();
        System.out.println("Current generation: " + currentGeneration);
        if (currentGeneration != 2) {
            throw new AssertionError("Expected generation 2");
        }
        SyncGroupRequestHandler syncHandler = new SyncGroupRequestHandler(coordinator);

        SyncGroupRequestPayload stalePayload = new SyncGroupRequestPayload("orders-group", "consumer-A", 1);
        Request staleRequest = new Request(Request.SYNC_GROUP, (short) 1, 100, stalePayload.encode());
        Response staleResponse = syncHandler.handle(staleRequest);
        System.out.println("Stale generation response status: " + staleResponse.status());
        System.out.println("Stale generation response message: " + new String(staleResponse.payload()));


        if (staleResponse.status() != Response.ERROR) {
            throw new AssertionError("Stale generation should return ERROR");
        }

        if (staleResponse.correlationId() != 100) {
            throw new AssertionError("Correlation ID mismatch for stale request");
        }
        SyncGroupRequestPayload validPayload = new SyncGroupRequestPayload("orders-group", "consumer-A", 2);
        Request validRequest = new Request(Request.SYNC_GROUP, (short) 1, 101, validPayload.encode());
        Response validResponse = syncHandler.handle(validRequest);
        System.out.println("Current generation response status: " + validResponse.status());

        if (validResponse.status() != Response.SUCCESS) {
            throw new AssertionError("Current generation should be accepted");
        }

        if (validResponse.correlationId() != 101) {
            throw new AssertionError("Correlation ID mismatch for valid request");
        }
        SyncGroupResponsePayload responsePayload = SyncGroupResponsePayload.decode(validResponse.payload());
        System.out.println("Consumer-A assignment: " + responsePayload.partitions());
        if (responsePayload.partitions().isEmpty()) {
            throw new AssertionError("Consumer-A should have partitions");
        }
        System.out.println();
        System.out.println("SYNC_GROUP generation validation " + "verified successfully!");
    }
}
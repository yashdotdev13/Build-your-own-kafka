package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroupCoordinator;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.SyncGroupRequestHandler;
import com.buildyourownkafka.broker.SyncGroupRequestPayload;
import com.buildyourownkafka.broker.SyncGroupResponsePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.util.List;

public class SyncGroupRequestHandlerTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== SYNC GROUP REQUEST HANDLER TEST ===");
        ConsumerGroupManager groupManager = new ConsumerGroupManager();
        ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator(groupManager);
        coordinator.joinGroup("orders-group", "consumer-A", 4);
        coordinator.joinGroup("orders-group", "consumer-B", 4);
        SyncGroupRequestHandler handler = new SyncGroupRequestHandler(coordinator);
        SyncGroupRequestPayload payload = new SyncGroupRequestPayload("orders-group", "consumer-A", 0);
        Request request = new Request(Request.SYNC_GROUP, (short) 1, 100, payload.encode());
        Response response = handler.handle(request);

        if (response.status() != Response.SUCCESS) {
            throw new AssertionError("SYNC_GROUP request failed");
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
        if (!responsePayload.partitions().equals(List.of(0, 2))) {
            throw new AssertionError("Partition assignment mismatch");
        }
        if (response.correlationId() != 100) {
            throw new AssertionError("Correlation ID mismatch");
        }
        System.out.println();
        System.out.println("SYNC_GROUP request handler " + "verified successfully!");
    }
}
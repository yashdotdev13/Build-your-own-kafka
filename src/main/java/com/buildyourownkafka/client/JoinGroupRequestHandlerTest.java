package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroupCoordinator;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.JoinGroupRequestHandler;
import com.buildyourownkafka.broker.JoinGroupRequestPayload;
import com.buildyourownkafka.broker.JoinGroupResponsePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.util.List;

public class JoinGroupRequestHandlerTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== JOIN GROUP REQUEST HANDLER TEST ===");
        ConsumerGroupManager manager = new ConsumerGroupManager();
        ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator(manager);
        JoinGroupRequestHandler handler = new JoinGroupRequestHandler(coordinator);
        JoinGroupRequestPayload payload = new JoinGroupRequestPayload("orders-group", "consumer-A", 4);
        Request request = new Request(Request.JOIN_GROUP, (short) 1, 100, payload.encode());
        Response response = handler.handle(request);
        if (response.status() != Response.SUCCESS) {
            throw new RuntimeException("JOIN_GROUP request should succeed");
        }
        if (response.correlationId() != 100) {
            throw new RuntimeException("Correlation ID mismatch");
        }
        JoinGroupResponsePayload responsePayload = JoinGroupResponsePayload.decode(response.payload());
        System.out.println("Member ID: " + responsePayload.memberId());
        System.out.println("Generation: " + responsePayload.generation());
        System.out.println("Partitions: " + responsePayload.partitions());

        if (!responsePayload.memberId().equals("consumer-A")) {
            throw new RuntimeException("Incorrect member ID");
        }
        if (responsePayload.generation() != 0) {
            throw new RuntimeException("Initial generation should be 0");
        }
        if (!responsePayload.partitions().equals(List.of(0, 1, 2, 3))) {
            throw new RuntimeException("Incorrect partition assignment");
        }
        if (!manager.groupExists("orders-group")) {
            throw new RuntimeException("Consumer group should exist");
        }
        if (!manager.getGroup("orders-group").hasMember("consumer-A")) {
            throw new RuntimeException("Consumer-A should be registered");
        }

        System.out.println();
        System.out.println("JOIN_GROUP request handler " + "verified successfully!");
    }
}
package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

public class HeartbeatRequestHandler implements RequestHandler {

    private final ConsumerGroupCoordinator coordinator;

    public HeartbeatRequestHandler(ConsumerGroupCoordinator coordinator) {
        if (coordinator == null) {
            throw new IllegalArgumentException("Consumer group coordinator cannot be null");
        }
        this.coordinator = coordinator;
    }

    @Override
    public Response handle(Request request) {
        try {
            HeartbeatRequestPayload payload = HeartbeatRequestPayload.decode(request.payload());
            System.out.println("Decoded HEARTBEAT: " + "group=" + payload.groupId() + ", member="
                    + payload.memberId() + ", generation=" + payload.generation());

            coordinator.heartbeat(payload.groupId(), payload.memberId());
            HeartbeatResponsePayload responsePayload = new HeartbeatResponsePayload(payload.memberId(),
                    payload.generation());
            return new Response(request.correlationId(), Response.SUCCESS, responsePayload.encode());

        } catch (IllegalArgumentException e) {
            System.err.println("Failed to process HEARTBEAT: " + e.getMessage());
            return new Response(request.correlationId(), Response.ERROR, e.getMessage().getBytes());
        } catch (ConsumerGroupException e) {
            System.err.println("HEARTBEAT group error: " + e.getMessage());
            return new Response(request.correlationId(), Response.ERROR, e.getMessage().getBytes());
        } catch (Exception e) {
            System.err.println("Unexpected HEARTBEAT error: " + e.getMessage());
            return new Response(request.correlationId(), Response.ERROR, "Internal HEARTBEAT error".getBytes());
        }
    }
}
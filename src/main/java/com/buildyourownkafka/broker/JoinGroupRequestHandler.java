package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

public class JoinGroupRequestHandler implements RequestHandler {

    private final ConsumerGroupCoordinator coordinator;

    public JoinGroupRequestHandler(ConsumerGroupCoordinator coordinator) {
        if (coordinator == null) {
            throw new IllegalArgumentException("Consumer group coordinator cannot be null");
        }
        this.coordinator = coordinator;
    }

    @Override
    public Response handle(Request request) {

        try {
            JoinGroupRequestPayload payload = JoinGroupRequestPayload.decode(request.payload());
            System.out.println("Decoded JOIN_GROUP: group=" + payload.groupId() + ", member=" + payload.memberId() + ", partitions=" + payload.partitionCount());
            JoinGroupResult result = coordinator.joinGroup(payload.groupId(), payload.memberId(), payload.partitionCount());
            JoinGroupResponsePayload responsePayload = new JoinGroupResponsePayload(result.memberId(),
                    result.generation(), result.partitions());
            byte[] encodedResponse = responsePayload.encode();
            return new Response(request.correlationId(), Response.SUCCESS, encodedResponse);
        } catch (IllegalArgumentException e) {
            System.err.println("Failed to process JOIN_GROUP: " + e.getMessage());
            return new Response(request.correlationId(), Response.ERROR, e.getMessage().getBytes());
        } catch (Exception e) {
            System.err.println("Unexpected JOIN_GROUP error: " + e.getMessage());
            return new Response(request.correlationId(), Response.ERROR, "Internal JOIN_GROUP error".getBytes());
        }
    }
}
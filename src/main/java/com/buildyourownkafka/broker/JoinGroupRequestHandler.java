package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.ProtocolErrors;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.io.IOException;

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
            JoinGroupResult result = coordinator.joinGroup(payload.groupId(), payload.memberId(),
                    payload.partitionCount());
            JoinGroupResponsePayload responsePayload = new JoinGroupResponsePayload(result.memberId(),
                    result.generation(), result.partitions());
            return new Response(request.correlationId(), Response.SUCCESS, responsePayload.encode());
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid JOIN_GROUP request: correlationId=" + request.correlationId() +
                    ", reason=" + e.getMessage());
            return ProtocolErrors.clientError(request.correlationId(), e.getMessage());
        } catch (ConsumerGroupException e) {
            return ProtocolErrors.clientError(request.correlationId(), e.getMessage());
        } catch (IOException e) {
            System.err.println("Failed to encode JOIN_GROUP response: correlationId=" +
                    request.correlationId() + ", message=" + e.getMessage());
            return ProtocolErrors.internalError(request.correlationId());
        } catch (RuntimeException e) {
            System.err.println("Unexpected JOIN_GROUP failure: correlationId=" + request.correlationId() + ", exception=" +
                    e.getClass().getName() + ", message=" + e.getMessage());
            return ProtocolErrors.internalError(request.correlationId());
        }
    }
}

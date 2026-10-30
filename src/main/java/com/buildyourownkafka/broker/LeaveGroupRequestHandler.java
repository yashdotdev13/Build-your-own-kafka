package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.ProtocolErrors;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.io.IOException;

public class LeaveGroupRequestHandler implements RequestHandler {

    private final ConsumerGroupCoordinator coordinator;

    public LeaveGroupRequestHandler(ConsumerGroupCoordinator coordinator) {
        if (coordinator == null) {
            throw new IllegalArgumentException("Consumer group coordinator cannot be null");
        }
        this.coordinator = coordinator;
    }

    @Override
    public Response handle(Request request) {
        try {
            LeaveGroupRequestPayload payload = LeaveGroupRequestPayload.decode(request.payload());

            coordinator.leaveGroup(payload.groupId(), payload.memberId(), payload.partitionCount());
            LeaveGroupResponsePayload responsePayload = new LeaveGroupResponsePayload(payload.memberId());
            return new Response(request.correlationId(), Response.SUCCESS, responsePayload.encode());
        } catch (IllegalArgumentException e) {
            return ProtocolErrors.clientError(request.correlationId(), e.getMessage());
        } catch (ConsumerGroupException e) {
            return ProtocolErrors.clientError(request.correlationId(), e.getMessage());

        } catch (IOException e) {
            System.err.println("Failed to encode LEAVE_GROUP response: correlationId=" +
                    request.correlationId() + ", message=" + e.getMessage());
            return ProtocolErrors.internalError(request.correlationId());
        } catch (RuntimeException e) {
            System.err.println("Unexpected LEAVE_GROUP failure: correlationId=" + request.correlationId() + ", exception=" +
                    e.getClass().getName() + ", message=" + e.getMessage());
            return ProtocolErrors.internalError(request.correlationId());
        }
    }
}

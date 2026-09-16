package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

public class LeaveGroupRequestHandler implements RequestHandler {

    private final ConsumerGroupCoordinator coordinator;

    public LeaveGroupRequestHandler(
            ConsumerGroupCoordinator coordinator
    ) {
        if (coordinator == null) {
            throw new IllegalArgumentException(
                    "Consumer group coordinator cannot be null"
            );
        }

        this.coordinator = coordinator;
    }

    @Override
    public Response handle(Request request) {

        if (request == null) {
            throw new IllegalArgumentException(
                    "Request cannot be null"
            );
        }

        if (request.type() != Request.LEAVE_GROUP) {
            throw new IllegalArgumentException(
                    "Invalid request type: " + request.type()
            );
        }

        try {

            LeaveGroupRequestPayload payload =
                    LeaveGroupRequestPayload.decode(
                            request.payload()
                    );

            coordinator.leaveGroup(
                    payload.groupId(),
                    payload.memberId(),
                    payload.partitionCount()
            );

            LeaveGroupResponsePayload responsePayload =
                    new LeaveGroupResponsePayload(
                            payload.memberId()
                    );

            return new Response(
                    request.correlationId(),
                    Response.SUCCESS,
                    responsePayload.encode()
            );

        } catch (IllegalArgumentException e) {

            return new Response(
                    request.correlationId(),
                    Response.ERROR,
                    e.getMessage().getBytes()
            );

        } catch (ConsumerGroupException e) {

            return new Response(
                    request.correlationId(),
                    Response.ERROR,
                    e.getMessage().getBytes()
            );

        } catch (Exception e) {

            return new Response(
                    request.correlationId(),
                    Response.ERROR,
                    ("Internal LEAVE_GROUP error: "
                            + e.getMessage()).getBytes()
            );
        }
    }
}
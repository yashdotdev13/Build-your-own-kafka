package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.ProtocolErrors;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.io.IOException;

public class SyncGroupRequestHandler implements RequestHandler {

    private final ConsumerGroupCoordinator coordinator;

    public SyncGroupRequestHandler(ConsumerGroupCoordinator coordinator) {
        if (coordinator == null) {
            throw new IllegalArgumentException(
                    "Consumer group coordinator cannot be null"
            );
        }
        this.coordinator = coordinator;
    }

    @Override
    public Response handle(Request request) {
        try {
            SyncGroupRequestPayload payload =
                    SyncGroupRequestPayload.decode(request.payload());

            JoinGroupResult result = coordinator.syncGroup(
                    payload.groupId(),
                    payload.memberId(),
                    payload.generation()
            );

            SyncGroupResponsePayload responsePayload =
                    new SyncGroupResponsePayload(
                            result.memberId(),
                            result.generation(),
                            result.partitions()
                    );

            return new Response(
                    request.correlationId(),
                    Response.SUCCESS,
                    responsePayload.encode()
            );

        } catch (IllegalArgumentException e) {
            return ProtocolErrors.clientError(
                    request.correlationId(),
                    e.getMessage()
            );
        } catch (ConsumerGroupException e) {
            return ProtocolErrors.clientError(
                    request.correlationId(),
                    e.getMessage()
            );
        } catch (RuntimeException e) {
            System.err.println(
                    "Unexpected SYNC_GROUP failure: correlationId="
                            + request.correlationId()
                            + ", exception=" + e.getClass().getName()
                            + ", message=" + e.getMessage()
            );

            return ProtocolErrors.internalError(
                    request.correlationId()
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

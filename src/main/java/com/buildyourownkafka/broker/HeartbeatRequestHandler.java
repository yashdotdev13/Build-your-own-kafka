
package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.ProtocolErrors;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.io.IOException;

public class HeartbeatRequestHandler implements RequestHandler {

    private final ConsumerGroupCoordinator coordinator;

    public HeartbeatRequestHandler(ConsumerGroupCoordinator coordinator) {
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
            HeartbeatRequestPayload payload =
                    HeartbeatRequestPayload.decode(request.payload());

            coordinator.heartbeat(
                    payload.groupId(),
                    payload.memberId()
            );

            HeartbeatResponsePayload responsePayload =
                    new HeartbeatResponsePayload(
                            payload.memberId(),
                            payload.generation()
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

        } catch (IOException e) {
            System.err.println(
                    "Failed to encode HEARTBEAT response: correlationId="
                            + request.correlationId()
                            + ", message=" + e.getMessage()
            );

            return ProtocolErrors.internalError(
                    request.correlationId()
            );

        } catch (RuntimeException e) {
            System.err.println(
                    "Unexpected HEARTBEAT failure: correlationId="
                            + request.correlationId()
                            + ", exception=" + e.getClass().getName()
                            + ", message=" + e.getMessage()
            );

            return ProtocolErrors.internalError(
                    request.correlationId()
            );
        }
    }
}

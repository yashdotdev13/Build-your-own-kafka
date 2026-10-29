
package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.FetchOffsetPayload;
import com.buildyourownkafka.protocol.FetchOffsetResponsePayload;
import com.buildyourownkafka.protocol.ProtocolErrors;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

public class FetchOffsetRequestHandler implements RequestHandler {

    private final ConsumerOffsetStore consumerOffsetStore;

    public FetchOffsetRequestHandler(
            ConsumerOffsetStore consumerOffsetStore
    ) {
        if (consumerOffsetStore == null) {
            throw new IllegalArgumentException(
                    "Consumer offset store cannot be null"
            );
        }

        this.consumerOffsetStore = consumerOffsetStore;
    }

    @Override
    public Response handle(Request request) {
        try {
            FetchOffsetPayload payload =
                    FetchOffsetPayload.decode(request.payload());

            long offset = consumerOffsetStore.fetch(
                    payload.groupId(),
                    payload.topic(),
                    payload.partition()
            );

            FetchOffsetResponsePayload responsePayload =
                    new FetchOffsetResponsePayload(offset);

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
        } catch (RuntimeException e) {
            System.err.println(
                    "Unexpected FETCH_OFFSET failure: correlationId="
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

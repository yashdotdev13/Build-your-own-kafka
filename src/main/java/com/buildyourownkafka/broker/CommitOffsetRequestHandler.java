package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.CommitOffsetPayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

public class CommitOffsetRequestHandler implements RequestHandler {
    private final ConsumerOffsetStore consumerOffsetStore;
    public CommitOffsetRequestHandler(ConsumerOffsetStore consumerOffsetStore) {
        if (consumerOffsetStore == null) {
            throw new IllegalArgumentException("Consumer offset store cannot be null");
        }
        this.consumerOffsetStore = consumerOffsetStore;
    }
    @Override
    public Response handle(Request request) {

        try {
            CommitOffsetPayload payload = CommitOffsetPayload.decode(request.payload());
            consumerOffsetStore.commit(payload.groupId(), payload.topic(), payload.partition(), payload.offset());
            return new Response(request.correlationId(), Response.SUCCESS, new byte[0]);
        } catch (Exception e) {
            return new Response(request.correlationId(), Response.ERROR, ("Failed to commit offset: " + e.getMessage()).getBytes());
        }
    }
}
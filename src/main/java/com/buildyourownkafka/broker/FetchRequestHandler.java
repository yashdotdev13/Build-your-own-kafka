package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.FetchPayload;
import com.buildyourownkafka.protocol.FetchResponsePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class FetchRequestHandler implements RequestHandler {
    private final TopicManager topicManager;

    public FetchRequestHandler(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    @Override
    public Response handle(Request request) {
        try {
            // 1. Decode FETCH payload
            FetchPayload payload = FetchPayload.decode(request.payload());
            // 2. Find topic
            Topic topic = topicManager.getTopic(payload.topicName());

            if (topic == null) {
                return new Response(request.correlationId(), Response.ERROR, ("Topic does not exist: " + payload.topicName()).getBytes(StandardCharsets.UTF_8));
            }
            // 3. Find partition
            Partition partition = topic.getPartition(payload.partitionId());

            // 4. Read records from requested offset
            List<Record> records = partition.readFrom(payload.offset());

            // 5. Create response payload
            FetchResponsePayload responsePayload = new FetchResponsePayload(records);

            // 6. Return response
            return new Response(request.correlationId(), Response.SUCCESS, responsePayload.encode());
        } catch (IllegalArgumentException e) {
            return new Response(request.correlationId(), Response.ERROR, e.getMessage().getBytes(StandardCharsets.UTF_8));
        }
    }
}
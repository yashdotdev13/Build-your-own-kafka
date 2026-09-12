package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.ProducePayload;
import com.buildyourownkafka.protocol.ProduceResponsePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

public class ProduceRequestHandler implements RequestHandler {

    private final TopicManager topicManager;

    public ProduceRequestHandler(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    @Override
    public Response handle(Request request) {
        try {
            // 1. Decode request payload
            ProducePayload payload = ProducePayload.decode(request.payload());
            // 2. Find topic
            Topic topic = topicManager.getTopic(payload.topicName());
            if (topic == null) {
                return new Response(request.correlationId(), Response.ERROR, ("Topic does not exist: " + payload.topicName()).getBytes());
            }
            // 3. Find partition
            Partition partition = topic.getPartition(payload.partitionId());
            // 4. Append record
            Record record = partition.append(payload.value());
            // 5. Create response payload
            ProduceResponsePayload responsePayload = new ProduceResponsePayload(record.offset());
            // 6. Return successful response
            return new Response(request.correlationId(), Response.SUCCESS, responsePayload.encode());
        } catch (IllegalArgumentException e) {
            return new Response(request.correlationId(), Response.ERROR, e.getMessage().getBytes());
        }
    }
}
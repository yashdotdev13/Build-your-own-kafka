package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.MetadataPayload;
import com.buildyourownkafka.protocol.MetadataResponsePayload;
import com.buildyourownkafka.protocol.ProtocolErrors;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.util.List;

public class MetadataRequestHandler implements RequestHandler {

    private final TopicManager topicManager;

    public MetadataRequestHandler(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    @Override
    public Response handle(Request request) {
        try {
            MetadataPayload payload = MetadataPayload.decode(request.payload());
            Topic topic = topicManager.getTopic(payload.topicName());
            if (topic == null) {
                return ProtocolErrors.clientError(request.correlationId(), "Topic does not exist: "
                        + payload.topicName());
            }
            List<Integer> partitionIds = topic.partitions().stream().map(Partition::id).toList();
            MetadataResponsePayload responsePayload = new MetadataResponsePayload(topic.name(), partitionIds);
            return new Response(request.correlationId(), Response.SUCCESS, responsePayload.encode());
        } catch (IllegalArgumentException e) {
            return ProtocolErrors.clientError(request.correlationId(), e.getMessage());
        }
    }
}

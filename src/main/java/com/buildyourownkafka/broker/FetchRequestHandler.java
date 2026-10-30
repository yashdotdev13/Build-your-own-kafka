package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.FetchPayload;
import com.buildyourownkafka.protocol.FetchResponsePayload;
import com.buildyourownkafka.protocol.ProtocolErrors;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.util.List;

public class FetchRequestHandler implements RequestHandler {

    private final TopicManager topicManager;

    public FetchRequestHandler(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    @Override
    public Response handle(Request request) {
        try {
            FetchPayload payload = FetchPayload.decode(request.payload());
            Topic topic = topicManager.getTopic(payload.topicName());
            if (topic == null) {
                return ProtocolErrors.clientError(request.correlationId(),
                        "Topic does not exist: " + payload.topicName());
            }
            Partition partition = topic.getPartition(payload.partitionId());
            List<Record> records = partition.readFrom(payload.offset(), payload.maxRecords());
            FetchResponsePayload responsePayload = new FetchResponsePayload(records);
            return new Response(request.correlationId(), Response.SUCCESS, responsePayload.encode());
        } catch (IllegalArgumentException e) {
            return ProtocolErrors.clientError(request.correlationId(), e.getMessage());
        }
    }
}

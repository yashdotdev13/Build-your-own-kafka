package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.ProducePayload;
import com.buildyourownkafka.protocol.ProduceResponsePayload;
import com.buildyourownkafka.protocol.ProtocolErrors;
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
            ProducePayload payload = ProducePayload.decode(request.payload());

            Topic topic = topicManager.getTopic(payload.topicName());

            if (topic == null) {
                return ProtocolErrors.clientError(
                        request.correlationId(),
                        "Topic does not exist: " + payload.topicName()
                );
            }

            Partition partition = topic.getPartition(payload.partitionId());

            Record record = partition.append(payload.value());

            ProduceResponsePayload responsePayload =
                    new ProduceResponsePayload(record.offset());

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
        }
    }
}

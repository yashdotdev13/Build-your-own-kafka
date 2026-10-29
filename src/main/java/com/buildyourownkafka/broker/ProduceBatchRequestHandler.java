
package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.ProduceBatchPayload;
import com.buildyourownkafka.protocol.ProduceBatchResponsePayload;
import com.buildyourownkafka.protocol.ProtocolErrors;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

public class ProduceBatchRequestHandler implements RequestHandler {

    private final TopicManager topicManager;

    public ProduceBatchRequestHandler(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    @Override
    public Response handle(Request request) {
        try {
            ProduceBatchPayload payload =
                    ProduceBatchPayload.decode(request.payload());

            Topic topic = topicManager.getTopic(payload.topicName());

            if (topic == null) {
                return ProtocolErrors.clientError(
                        request.correlationId(),
                        "Topic does not exist: " + payload.topicName()
                );
            }

            Partition partition =
                    topic.getPartition(payload.partitionId());

            long baseOffset;
            int recordCount = payload.records().size();

            synchronized (partition) {
                baseOffset = partition.nextOffset();

                for (byte[] value : payload.records()) {
                    partition.append(value);
                }
            }

            ProduceBatchResponsePayload responsePayload =
                    new ProduceBatchResponsePayload(
                            baseOffset,
                            recordCount
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
        }
    }
}

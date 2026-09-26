package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.MetadataPayload;
import com.buildyourownkafka.protocol.MetadataResponsePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class MetadataRequestHandler implements RequestHandler {

    private final TopicManager topicManager;

    public MetadataRequestHandler(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    @Override
    public Response handle(Request request) {

        try {
            MetadataPayload payload =
                    MetadataPayload.decode(request.payload());

            Topic topic =
                    topicManager.getTopic(payload.topicName());

            if (topic == null) {
                return new Response(
                        request.correlationId(),
                        Response.ERROR,
                        (
                                "Topic does not exist: "
                                        + payload.topicName()
                        ).getBytes(StandardCharsets.UTF_8)
                );
            }

            List<Integer> partitionIds =
                    topic.partitions()
                            .stream()
                            .map(Partition::id)
                            .toList();

            MetadataResponsePayload responsePayload =
                    new MetadataResponsePayload(
                            topic.name(),
                            partitionIds
                    );

            return new Response(
                    request.correlationId(),
                    Response.SUCCESS,
                    responsePayload.encode()
            );

        } catch (IllegalArgumentException e) {

            return new Response(
                    request.correlationId(),
                    Response.ERROR,
                    e.getMessage()
                            .getBytes(StandardCharsets.UTF_8)
            );
        }
    }
}



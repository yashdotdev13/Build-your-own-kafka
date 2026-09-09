package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.CreateTopicPayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.nio.charset.StandardCharsets;

public class CreateTopicRequestHandler implements RequestHandler {

    private final TopicManager topicManager;

    public CreateTopicRequestHandler(
            TopicManager topicManager
    ) {
        this.topicManager = topicManager;
    }

    @Override
    public Response handle(Request request) {

        try {

            CreateTopicPayload payload =
                    CreateTopicPayload.decode(
                            request.payload()
                    );

            System.out.println(
                    "Decoded CREATE_TOPIC: "
                            + payload.topicName()
                            + ", partitions="
                            + payload.partitionCount()
            );

            Topic topic =
                    topicManager.createTopic(
                            payload.topicName(),
                            payload.partitionCount()
                    );

            System.out.println(
                    "Topic created: "
                            + topic.name()
                            + " with "
                            + topic.partitionCount()
                            + " partition(s)"
            );

            return new Response(
                    request.correlationId(),
                    Response.SUCCESS,
                    topic.name().getBytes(
                            StandardCharsets.UTF_8
                    )
            );

        } catch (IllegalArgumentException e) {

            System.err.println(
                    "Failed to create topic: "
                            + e.getMessage()
            );

            return new Response(
                    request.correlationId(),
                    Response.ERROR,
                    e.getMessage().getBytes(
                            StandardCharsets.UTF_8
                    )
            );
        }
    }
}
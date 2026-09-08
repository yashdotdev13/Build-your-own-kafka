package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.nio.charset.StandardCharsets;

public class CreateTopicRequestHandler implements RequestHandler {

    private final TopicManager topicManager;

    public CreateTopicRequestHandler(TopicManager topicManager) {
        this.topicManager = topicManager;
    }

    @Override
    public Response handle(Request request) {

        try {
            String topicName =
                    new String(request.payload(), StandardCharsets.UTF_8);

            Topic topic = topicManager.createTopic(topicName);

            System.out.println(
                    "Topic created: " + topic.name()
            );

            return new Response(
                    request.correlationId(),
                    Response.SUCCESS,
                    topic.name().getBytes(StandardCharsets.UTF_8)
            );

        } catch (IllegalArgumentException e) {

            System.err.println(
                    "Failed to create topic: " + e.getMessage()
            );

            return new Response(
                    request.correlationId(),
                    Response.ERROR,
                    e.getMessage().getBytes(StandardCharsets.UTF_8)
            );
        }
    }
}
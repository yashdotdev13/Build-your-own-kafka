package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.nio.charset.StandardCharsets;

public class CreateTopicRequestHandler
        implements RequestHandler {

    private final TopicManager topicManager;

    public CreateTopicRequestHandler(
            TopicManager topicManager
    ) {
        this.topicManager = topicManager;
    }

    @Override
    public Response handle(Request request) {

        String topicName =
                new String(
                        request.payload(),
                        StandardCharsets.UTF_8
                );

        try {

            Topic topic =
                    topicManager.createTopic(topicName);

            return new Response(
                    request.correlationId(),
                    Response.SUCCESS,
                    topic.name().getBytes(
                            StandardCharsets.UTF_8
                    )
            );

        } catch (IllegalArgumentException e) {

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
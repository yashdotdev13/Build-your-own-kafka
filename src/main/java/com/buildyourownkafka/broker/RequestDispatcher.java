package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class RequestDispatcher {

    private final Map<Integer, RequestHandler> handlers;

    public RequestDispatcher(
            TopicManager topicManager
    ) {

        this.handlers = new HashMap<>();

        register(
                Request.PING,
                new PingRequestHandler()
        );

        register(
                Request.CREATE_TOPIC,
                new CreateTopicRequestHandler(
                        topicManager
                )
        );
    }

    public void register(
            int requestType,
            RequestHandler handler
    ) {
        handlers.put(requestType, handler);
    }

    public Response dispatch(Request request) {

        RequestHandler handler =
                handlers.get(request.type());

        if (handler == null) {

            return new Response(
                    request.correlationId(),
                    Response.ERROR,
                    (
                            "Unknown request type: "
                                    + request.type()
                    ).getBytes(
                            StandardCharsets.UTF_8
                    )
            );
        }

        return handler.handle(request);
    }
}
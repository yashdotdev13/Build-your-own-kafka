package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.util.HashMap;
import java.util.Map;

public class RequestDispatcher {

    private final Map<Integer, RequestHandler> handlers = new HashMap<>();

    public RequestDispatcher(TopicManager topicManager, ConsumerOffsetStore consumerOffsetStore, ConsumerGroupCoordinator consumerGroupCoordinator) {

        register(Request.PING, new PingRequestHandler());
        register(Request.CREATE_TOPIC, new CreateTopicRequestHandler(topicManager));
        register(Request.PRODUCE, new ProduceRequestHandler(topicManager));
        register(Request.FETCH, new FetchRequestHandler(topicManager));
        register(Request.COMMIT_OFFSET, new CommitOffsetRequestHandler(consumerOffsetStore));
        register(Request.FETCH_OFFSET, new FetchOffsetRequestHandler(consumerOffsetStore));
        register(Request.JOIN_GROUP, new JoinGroupRequestHandler(consumerGroupCoordinator));
        register(
                Request.SYNC_GROUP,
                new SyncGroupRequestHandler(consumerGroupCoordinator)
        );
    }

    private void register(int requestType, RequestHandler handler) {
        handlers.put(requestType, handler);
    }

    public Response dispatch(Request request) {
        RequestHandler handler = handlers.get(request.type());
        if (handler == null) {
            return new Response(request.correlationId(), Response.ERROR, ("Unknown request type: " + request.type()).getBytes());
        }
        return handler.handle(request);
    }
}
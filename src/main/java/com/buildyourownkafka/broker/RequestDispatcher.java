package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.ProtocolErrors;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.util.HashMap;
import java.util.Map;

public class RequestDispatcher {

    private final Map<Integer, RequestHandler> handlers = new HashMap<>();

    public RequestDispatcher(
            TopicManager topicManager,
            ConsumerOffsetStore consumerOffsetStore,
            ConsumerGroupCoordinator consumerGroupCoordinator
    ) {
        register(Request.PING, new PingRequestHandler());
        register(Request.CREATE_TOPIC, new CreateTopicRequestHandler(topicManager));
        register(Request.PRODUCE, new ProduceRequestHandler(topicManager));
        register(Request.PRODUCE_BATCH, new ProduceBatchRequestHandler(topicManager));
        register(Request.FETCH, new FetchRequestHandler(topicManager));
        register(Request.COMMIT_OFFSET, new CommitOffsetRequestHandler(consumerOffsetStore));
        register(Request.FETCH_OFFSET, new FetchOffsetRequestHandler(consumerOffsetStore));
        register(Request.JOIN_GROUP, new JoinGroupRequestHandler(consumerGroupCoordinator));
        register(Request.SYNC_GROUP, new SyncGroupRequestHandler(consumerGroupCoordinator));
        register(Request.LEAVE_GROUP, new LeaveGroupRequestHandler(consumerGroupCoordinator));
        register(Request.HEARTBEAT, new HeartbeatRequestHandler(consumerGroupCoordinator));
        register(Request.METADATA, new MetadataRequestHandler(topicManager));
    }

    // Package-private constructor for dispatcher tests.
    RequestDispatcher(Map<Integer, RequestHandler> handlers) {
        this.handlers.putAll(handlers);
    }

    private void register(int requestType, RequestHandler handler) {
        handlers.put(requestType, handler);
    }

    public Response dispatch(Request request) {
        if (request == null) {
            return ProtocolErrors.clientError(
                    0,
                    "Invalid request"
            );
        }

        RequestHandler handler = handlers.get(request.type());

        if (handler == null) {
            return ProtocolErrors.clientError(
                    request.correlationId(),
                    "Unknown request type: " + request.type()
            );
        }

        try {
            Response response = handler.handle(request);

            if (response == null) {
                System.err.println(
                        "Request handler returned null: type="
                                + request.type()
                                + ", correlationId="
                                + request.correlationId()
                );

                return ProtocolErrors.internalError(
                        request.correlationId()
                );
            }

            return response;

        } catch (IllegalArgumentException e) {
            System.err.println(
                    "Invalid request: type="
                            + request.type()
                            + ", correlationId="
                            + request.correlationId()
                            + ", reason="
                            + e.getMessage()
            );

            return ProtocolErrors.clientError(
                    request.correlationId(),
                    e.getMessage()
            );

        } catch (RuntimeException e) {
            System.err.println(
                    "Unexpected request failure: type="
                            + request.type()
                            + ", correlationId="
                            + request.correlationId()
                            + ", exception="
                            + e.getClass().getName()
                            + ", message="
                            + e.getMessage()
            );

            return ProtocolErrors.internalError(
                    request.correlationId()
            );
        }
    }
}

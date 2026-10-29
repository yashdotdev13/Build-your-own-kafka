package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class RequestDispatcherTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldReturnErrorForUnknownRequestType() {
        RequestDispatcher dispatcher = createDispatcher();

        Request request = new Request(
                999,
                (short) 1,
                42,
                new byte[0]
        );

        Response response = dispatcher.dispatch(request);

        assertEquals(42, response.correlationId());
        assertEquals(Response.ERROR, response.status());
        assertEquals(
                "Unknown request type: 999",
                new String(response.payload(), StandardCharsets.UTF_8)
        );
    }

    @Test
    void shouldReturnErrorForMalformedCreateTopicPayload() {
        RequestDispatcher dispatcher = createDispatcher();

        Request request = new Request(
                Request.CREATE_TOPIC,
                (short) 1,
                43,
                new byte[]{1, 2}
        );

        Response response = dispatcher.dispatch(request);

        assertEquals(43, response.correlationId());
        assertEquals(Response.ERROR, response.status());
        assertFalse(response.payload().length == 0);
    }

    private RequestDispatcher createDispatcher() {
        TopicManager topicManager = new TopicManager(tempDir);
        ConsumerOffsetStore offsetStore =
                new ConsumerOffsetStore(tempDir.resolve("offsets"));
        ConsumerGroupManager groupManager = new ConsumerGroupManager();
        ConsumerGroupCoordinator coordinator =
                new ConsumerGroupCoordinator(groupManager);

        return new RequestDispatcher(
                topicManager,
                offsetStore,
                coordinator
        );
    }
}

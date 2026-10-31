package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestEncoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseDecoder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ClientConnectionTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldContinueProcessingRequestsAfterRequestLevelError()
            throws Exception {

        TopicManager topicManager =
                new TopicManager(tempDir.resolve("topics"));

        ConsumerOffsetStore offsetStore =
                new ConsumerOffsetStore(tempDir.resolve("offsets"));

        ConsumerGroupCoordinator coordinator =
                new ConsumerGroupCoordinator(new ConsumerGroupManager());

        try (ServerSocket server = new ServerSocket(0);
             Socket client = new Socket(
                     "127.0.0.1",
                     server.getLocalPort()
             );
             Socket brokerSide = server.accept()) {

            ClientConnection connection = new ClientConnection(
                    brokerSide,
                    topicManager,
                    offsetStore,
                    coordinator
            );

            Thread connectionThread = Thread.ofVirtual().start(() -> {
                try {
                    connection.handle();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            RequestEncoder encoder = new RequestEncoder(
                    new DataOutputStream(client.getOutputStream())
            );

            ResponseDecoder decoder = new ResponseDecoder(
                    new DataInputStream(client.getInputStream())
            );

            // First request: unsupported operation.
            encoder.encode(new Request(
                    999,
                    (short) 1,
                    100,
                    new byte[0]
            ));

            Response errorResponse = decoder.decode();

            assertNotNull(errorResponse);
            assertEquals(100, errorResponse.correlationId());
            assertEquals(Response.ERROR, errorResponse.status());

            // Second request: valid PING on the same connection.
            encoder.encode(new Request(
                    Request.PING,
                    (short) 1,
                    101,
                    new byte[0]
            ));

            Response pingResponse = decoder.decode();

            assertNotNull(pingResponse);
            assertEquals(101, pingResponse.correlationId());
            assertEquals(Response.SUCCESS, pingResponse.status());

            assertEquals(
                    "PONG",
                    new String(
                            pingResponse.payload(),
                            StandardCharsets.UTF_8
                    )
            );

            client.shutdownOutput();

            connectionThread.join(2_000);

            assertFalse(
                    connectionThread.isAlive(),
                    "Connection handler should finish after client EOF"
            );
        }
    }



    @Test
    void shouldKeepConnectionOpenWhileClientIsIdle()
            throws Exception {

        TopicManager topicManager =
                new TopicManager(tempDir.resolve("idle-topics"));

        ConsumerOffsetStore offsetStore =
                new ConsumerOffsetStore(tempDir.resolve("idle-offsets"));

        ConsumerGroupCoordinator coordinator =
                new ConsumerGroupCoordinator(new ConsumerGroupManager());

        try (ServerSocket server = new ServerSocket(0);
             Socket client = new Socket(
                     "127.0.0.1",
                     server.getLocalPort()
             );
             Socket brokerSide = server.accept()) {

            ClientConnection connection = new ClientConnection(
                    brokerSide,
                    topicManager,
                    offsetStore,
                    coordinator,
                    500
            );

            Thread connectionThread = Thread.ofVirtual().start(() -> {
                try (brokerSide) {
                    connection.handle();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            // Keep the client idle longer than the configured timeout.
            Thread.sleep(1_000);

            assertTrue(
                    connectionThread.isAlive(),
                    "Connection should remain open while client is idle"
            );

            assertFalse(
                    brokerSide.isClosed(),
                    "Broker-side socket should remain open while client is idle"
            );

            // Now send a valid PING over the same connection.
            RequestEncoder encoder = new RequestEncoder(
                    new DataOutputStream(client.getOutputStream())
            );

            ResponseDecoder decoder = new ResponseDecoder(
                    new DataInputStream(client.getInputStream())
            );

            encoder.encode(new Request(
                    Request.PING,
                    (short) 1,
                    200,
                    new byte[0]
            ));

            Response response = decoder.decode();

            assertNotNull(response);
            assertEquals(200, response.correlationId());
            assertEquals(Response.SUCCESS, response.status());

            assertEquals(
                    "PONG",
                    new String(response.payload(), StandardCharsets.UTF_8)
            );

            client.shutdownOutput();

            connectionThread.join(2_000);

            assertFalse(
                    connectionThread.isAlive(),
                    "Connection handler should finish after client EOF"
            );
        }
    }




    @Test
    void shouldCloseConnectionWhenTotalRequestDeadlineExpires()
            throws Exception {

        TopicManager topicManager =
                new TopicManager(tempDir.resolve("deadline-topics"));

        ConsumerOffsetStore offsetStore =
                new ConsumerOffsetStore(tempDir.resolve("deadline-offsets"));

        ConsumerGroupCoordinator coordinator =
                new ConsumerGroupCoordinator(new ConsumerGroupManager());

        int socketReadTimeoutMs = 2_000;
        int requestTimeoutMs = 500;

        try (ServerSocket server = new ServerSocket(0);
             Socket client = new Socket(
                     "127.0.0.1",
                     server.getLocalPort()
             );
             Socket brokerSide = server.accept()) {

            ClientConnection connection = new ClientConnection(
                    brokerSide,
                    topicManager,
                    offsetStore,
                    coordinator,
                    socketReadTimeoutMs,
                    requestTimeoutMs
            );

            Thread connectionThread = Thread.ofVirtual().start(() -> {
                try (brokerSide) {
                    connection.handle();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            DataOutputStream output = new DataOutputStream(
                    client.getOutputStream()
            );

            // Begin a frame by sending only its first length-prefix byte.
            output.writeByte(0);
            output.flush();

            // Keep the client stalled while the request deadline expires.
            Thread.sleep(requestTimeoutMs + 300L);

            connectionThread.join(2_000);

            assertFalse(
                    connectionThread.isAlive(),
                    "Connection should terminate when total request deadline expires"
            );

            assertTrue(
                    brokerSide.isClosed(),
                    "Broker-side socket should close after request deadline"
            );
        }
    }


}

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
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;

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
}

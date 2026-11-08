package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestEncoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseDecoder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class BrokerServerTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldRejectConnectionsWhenMaximumConnectionLimitIsReached()
            throws Exception {

        BrokerServer broker = new BrokerServer(
                0,
                1,
                tempDir.resolve("overload-test")
        );

        AtomicReference<Throwable> serverError = new AtomicReference<>();

        Thread brokerThread = startBroker(broker, serverError);

        try (Socket admittedClient = connectToBroker(broker);
             Socket rejectedClient = connectToBroker(broker)) {

            // The first client occupies the only available connection slot.
            assertTrue(admittedClient.isConnected());

            // The second client must be rejected.
            assertConnectionRejected(rejectedClient);

            // The broker must still process requests from the admitted client.
            assertPingResponse(admittedClient, 100);

        } finally {
            broker.stop();
            brokerThread.join(2_000);
        }

        assertFalse(
                brokerThread.isAlive(),
                "Broker thread should stop after closing its server socket"
        );

        assertNull(
                serverError.get(),
                "Broker should not fail unexpectedly"
        );
    }

    @Test
    void shouldReleaseConnectionPermitAfterClientDisconnects()
            throws Exception {

        BrokerServer broker = new BrokerServer(
                0,
                1,
                tempDir.resolve("permit-release-test")
        );

        AtomicReference<Throwable> serverError = new AtomicReference<>();

        Thread brokerThread = startBroker(broker, serverError);

        try {
            Socket firstClient = connectToBroker(broker);

            try (firstClient) {
                assertPingResponse(firstClient, 200);
            }

            // Wait until the first handler terminates and releases its permit.
            try (Socket secondClient = connectAndPingAfterPermitRelease(
                    broker,
                    201
            )) {
                assertTrue(secondClient.isConnected());
            }

        } finally {
            broker.stop();
            brokerThread.join(2_000);
        }

        assertFalse(
                brokerThread.isAlive(),
                "Broker thread should stop after closing its server socket"
        );

        assertNull(
                serverError.get(),
                "Broker should not fail unexpectedly"
        );
    }

    @Test
    void shouldRemainResponsiveDuringRepeatedConnectionOverload()
            throws Exception {

        BrokerServer broker = new BrokerServer(
                0,
                1,
                tempDir.resolve("stress-test")
        );

        AtomicReference<Throwable> serverError = new AtomicReference<>();

        Thread brokerThread = startBroker(broker, serverError);

        try (Socket admittedClient = connectToBroker(broker)) {

            // Generate repeated connection attempts beyond broker capacity.
            for (int i = 0; i < 25; i++) {
                try (Socket excessClient = connectToBroker(broker)) {
                    assertConnectionRejected(excessClient);
                }
            }

            // Existing admitted connection must remain responsive.
            assertPingResponse(admittedClient, 300);

        } finally {
            broker.stop();
            brokerThread.join(2_000);
        }

        assertFalse(
                brokerThread.isAlive(),
                "Broker thread should stop after closing its server socket"
        );

        assertNull(
                serverError.get(),
                "Broker should not fail unexpectedly"
        );
    }

    @Test
    void shouldRejectNonPositiveMaximumConnectionLimit() {

        assertThrows(
                IllegalArgumentException.class,
                () -> new BrokerServer(
                        0,
                        0,
                        tempDir.resolve("invalid-test")
                )
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new BrokerServer(
                        0,
                        -1,
                        tempDir.resolve("invalid-test")
                )
        );
    }

    private Thread startBroker(
            BrokerServer broker,
            AtomicReference<Throwable> serverError
    ) throws Exception {

        Thread brokerThread = Thread.ofVirtual().start(() -> {
            try {
                broker.start();
            } catch (Throwable e) {
                serverError.set(e);
            }
        });

        // Wait until the broker has successfully bound its socket.
        long deadline = System.nanoTime()
                + 5_000_000_000L;

        while (broker.getLocalPort() == -1
                && System.nanoTime() < deadline) {

            if (!brokerThread.isAlive()) {
                fail("Broker terminated before starting");
            }

            Thread.sleep(10);
        }

        assertTrue(
                broker.getLocalPort() > 0,
                "Broker should bind to an available port"
        );

        return brokerThread;
    }

    private Socket connectToBroker(BrokerServer broker)
            throws Exception {

        Socket socket = new Socket(
                "127.0.0.1",
                broker.getLocalPort()
        );

        socket.setSoTimeout(2_000);

        return socket;
    }

    private void assertConnectionRejected(Socket socket)
            throws Exception {

        int received = socket.getInputStream().read();

        assertEquals(
                -1,
                received,
                "Rejected connection should be closed by the broker"
        );
    }

    private void assertPingResponse(Socket socket, int correlationId)
            throws Exception {

        RequestEncoder encoder = new RequestEncoder(
                new DataOutputStream(socket.getOutputStream())
        );

        ResponseDecoder decoder = new ResponseDecoder(
                new DataInputStream(socket.getInputStream())
        );

        encoder.encode(new Request(
                Request.PING,
                (short) 1,
                correlationId,
                new byte[0]
        ));

        Response response = decoder.decode();

        assertNotNull(response);

        assertEquals(
                correlationId,
                response.correlationId()
        );

        assertEquals(
                Response.SUCCESS,
                response.status()
        );

        assertEquals(
                "PONG",
                new String(
                        response.payload(),
                        StandardCharsets.UTF_8
                )
        );
    }

    private Socket connectAndPingAfterPermitRelease(
            BrokerServer broker,
            int correlationId
    ) throws Exception {

        long deadline = System.nanoTime()
                + 5_000_000_000L;

        Exception lastException = null;

        while (System.nanoTime() < deadline) {

            Socket socket = null;

            try {
                socket = connectToBroker(broker);

                assertPingResponse(socket, correlationId);

                return socket;

            } catch (Exception e) {
                lastException = e;

                if (socket != null) {
                    try {
                        socket.close();
                    } catch (Exception ignored) {
                        // Preserve the original connection failure.
                    }
                }

                Thread.sleep(50);
            }
        }

        throw new AssertionError(
                "Broker did not release its connection permit in time",
                lastException
        );
    }
}

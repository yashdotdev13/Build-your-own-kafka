
package com.buildyourownkafka.broker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.concurrent.Semaphore;

public class BrokerServer {

    private final int port;
    private ServerSocket serverSocket;
    private volatile boolean running;

    private final TopicManager topicManager;
    private final ConsumerOffsetStore consumerOffsetStore;
    private final ConsumerGroupManager consumerGroupManager;
    private final ConsumerGroupCoordinator consumerGroupCoordinator;

    /**
     * Limits the number of concurrently active client connections.
     */
    private final Semaphore connectionPermits;

    public BrokerServer(int port) {
        this(port, BrokerNetworkConfig.MAX_CONNECTIONS);
    }

    /**
     * Creates a broker with a configurable maximum connection count.
     *
     * @param port broker listening port
     * @param maxConnections maximum concurrently active connections
     */
    public BrokerServer(int port, int maxConnections) {

        if (maxConnections <= 0) {
            throw new IllegalArgumentException(
                    "Maximum connections must be positive"
            );
        }

        this.port = port;
        this.connectionPermits = new Semaphore(maxConnections);

        this.topicManager = new TopicManager(Path.of("data", "topics"));
        this.consumerOffsetStore =
                new ConsumerOffsetStore(Path.of("data", "offsets"));
        this.consumerGroupManager = new ConsumerGroupManager();
        this.consumerGroupCoordinator =
                new ConsumerGroupCoordinator(consumerGroupManager);
    }

    public void start() throws IOException {

        serverSocket = new ServerSocket(port);
        running = true;

        System.out.println("========================================");
        System.out.println("        Build Your Own Kafka");
        System.out.println("========================================");
        System.out.println();
        System.out.println("Broker starting...");
        System.out.println("Port: " + port);
        System.out.println("Maximum connections: "
                + connectionPermits.availablePermits());
        System.out.println();
        System.out.println("Broker started successfully.");
        System.out.println("Waiting for connections...");

        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setSoTimeout(0);

                if (!connectionPermits.tryAcquire()) {
                    rejectConnection(clientSocket);
                    continue;
                }

                System.out.println(
                        "Client connected: "
                                + clientSocket.getRemoteSocketAddress()
                );

                try {
                    Thread.startVirtualThread(
                            () -> handleClient(clientSocket)
                    );
                } catch (RuntimeException | Error e) {
                    connectionPermits.release();

                    try {
                        clientSocket.close();
                    } catch (IOException closeException) {
                        e.addSuppressed(closeException);
                    }

                    throw e;
                }

            } catch (IOException e) {
                if (running) {
                    System.err.println(
                            "Error accepting client connection: "
                                    + e.getMessage()
                    );
                }
            }
        }
    }

    private void rejectConnection(Socket clientSocket) {

        System.err.println(
                "Connection rejected: broker connection limit reached. "
                        + clientSocket.getRemoteSocketAddress()
        );

        try {
            clientSocket.close();
        } catch (IOException e) {
            System.err.println(
                    "Error closing rejected connection: " + e.getMessage()
            );
        }
    }

    private void handleClient(Socket clientSocket) {

        try (clientSocket) {

            ClientConnection connection = new ClientConnection(
                    clientSocket,
                    topicManager,
                    consumerOffsetStore,
                    consumerGroupCoordinator
            );

            connection.handle();

        } catch (IOException e) {
            System.err.println(
                    "Client connection error: " + e.getMessage()
            );
        } finally {
            connectionPermits.release();
        }
    }

    public void stop() {

        running = false;

        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                System.err.println(
                        "Error while stopping broker: " + e.getMessage()
                );
            }
        }

        System.out.println("Broker stopped.");
    }
}

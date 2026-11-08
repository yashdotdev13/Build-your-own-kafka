package com.buildyourownkafka.broker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.Semaphore;

public class BrokerServer {

    private final int port;
    private volatile ServerSocket serverSocket;
    private volatile boolean running;

    private final TopicManager topicManager;
    private final ConsumerOffsetStore consumerOffsetStore;
    private final ConsumerGroupManager consumerGroupManager;
    private final ConsumerGroupCoordinator consumerGroupCoordinator;

    /**
     * Maximum number of concurrently active client connections.
     */
    private final int maxConnections;

    /**
     * Controls admission of client connections.
     */
    private final Semaphore connectionPermits;

    public BrokerServer(int port) {
        this(port, BrokerNetworkConfig.MAX_CONNECTIONS);
    }

    public BrokerServer(int port, int maxConnections) {
        this(port, maxConnections, Path.of("data"));
    }

    /**
     * Creates a broker with configurable connection limits and storage.
     *
     * @param port broker listening port
     * @param maxConnections maximum concurrently active connections
     * @param dataDirectory root directory for broker data
     */
    BrokerServer(int port, int maxConnections, Path dataDirectory) {

        if (maxConnections <= 0) {
            throw new IllegalArgumentException(
                    "Maximum connections must be positive"
            );
        }

        this.port = port;
        this.maxConnections = maxConnections;
        this.connectionPermits = new Semaphore(maxConnections);

        Path storageDirectory = Objects.requireNonNull(dataDirectory);

        this.topicManager = new TopicManager(
                storageDirectory.resolve("topics")
        );

        this.consumerOffsetStore = new ConsumerOffsetStore(
                storageDirectory.resolve("offsets")
        );

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
        System.out.println("Port: " + serverSocket.getLocalPort());
        System.out.println("Maximum connections: " + maxConnections);
        System.out.println();
        System.out.println("Broker started successfully.");
        System.out.println("Waiting for connections...");

        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();

                // Reject connections immediately when capacity is exhausted.
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

                    // Return the permit if the handler could not start.
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

    /**
     * Rejects a client connection when the broker reaches its limit.
     */
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

    /**
     * Handles a client connection using a virtual thread.
     */
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

            // Release capacity after the client connection terminates.
            connectionPermits.release();
        }
    }

    /**
     * Returns the actual bound port.
     * Useful when the broker starts with port 0.
     *
     * @return local port, or -1 if the server socket is not initialized
     */
    int getLocalPort() {
        return serverSocket == null ? -1 : serverSocket.getLocalPort();
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

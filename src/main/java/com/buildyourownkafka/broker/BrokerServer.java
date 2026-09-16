package com.buildyourownkafka.broker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;

public class BrokerServer {

    private final int port;
    private ServerSocket serverSocket;
    private volatile boolean running;
    private final TopicManager topicManager;
    private final ConsumerOffsetStore consumerOffsetStore;
    private final ConsumerGroupManager consumerGroupManager;
    private final ConsumerGroupCoordinator consumerGroupCoordinator;

    public BrokerServer(int port) {

        this.port = port;
        this.topicManager = new TopicManager(Path.of("data", "topics"));

        this.consumerOffsetStore = new ConsumerOffsetStore(Path.of("data", "offsets"));
        this.consumerGroupManager = new ConsumerGroupManager();
        this.consumerGroupCoordinator = new ConsumerGroupCoordinator(consumerGroupManager);
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
        System.out.println();
        System.out.println("Broker started successfully.");
        System.out.println("Waiting for connections...");
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected: " + clientSocket.getRemoteSocketAddress());
                Thread.startVirtualThread(() -> handleClient(clientSocket));
            } catch (IOException e) {
                if (running) {
                    System.err.println("Error accepting client connection: " + e.getMessage());
                }
            }
        }
    }

    private void handleClient(Socket clientSocket) {

        try (clientSocket) {
            ClientConnection connection = new ClientConnection(clientSocket, topicManager, consumerOffsetStore,
                    consumerGroupCoordinator);
            connection.handle();
        } catch (IOException e) {
            System.err.println("Client connection error: " + e.getMessage());
        }
    }

    public void stop() {

        running = false;
        if (serverSocket != null && !serverSocket.isClosed()) {

            try {
                serverSocket.close();

            } catch (IOException e) {
                System.err.println("Error while stopping broker: " + e.getMessage());
            }
        }
        System.out.println("Broker stopped.");
    }
}
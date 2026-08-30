package com.buildyourownkafka.broker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class BrokerServer {

    private final int port;
    private ServerSocket serverSocket;
    private volatile boolean running;

    public BrokerServer(int port) {
        this.port = port;
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

                System.out.println(
                        "Client connected: " +
                                clientSocket.getRemoteSocketAddress()
                );

                Thread.startVirtualThread(
                        () -> handleClient(clientSocket)
                );

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

    private void handleClient(Socket clientSocket) {
        try (clientSocket) {

            ClientConnection connection =
                    new ClientConnection(clientSocket);

            connection.handle();

        } catch (IOException e) {
            System.err.println(
                    "Client connection error: "
                            + e.getMessage()
            );
        }
    }

    public void stop() {
        running = false;

        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                System.err.println(
                        "Error while stopping broker: "
                                + e.getMessage()
                );
            }
        }

        System.out.println("Broker stopped.");
    }
}
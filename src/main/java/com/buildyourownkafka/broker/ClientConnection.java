package com.buildyourownkafka.broker;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class ClientConnection {

    private final Socket socket;

    public ClientConnection(Socket socket) {
        this.socket = socket;
    }

    public void handle() throws IOException {

        InputStream inputStream = socket.getInputStream();

        byte[] buffer = new byte[1024];

        int bytesRead = inputStream.read(buffer);

        if (bytesRead == -1) {
            System.out.println(
                    "Client disconnected: "
                            + socket.getRemoteSocketAddress()
            );
            return;
        }

        String message = new String(
                buffer,
                0,
                bytesRead,
                StandardCharsets.UTF_8
        );

        System.out.println(
                "Received from client: " + message
        );
    }
}
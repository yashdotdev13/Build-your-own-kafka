package com.buildyourownkafka.client;

import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class TestClient {

    public static void main(String[] args) throws Exception {

        try (Socket socket = new Socket("localhost", 9092)) {

            System.out.println("Connected to broker.");

            OutputStream outputStream =
                    socket.getOutputStream();

            String message = "Hello Broker";

            outputStream.write(
                    message.getBytes(StandardCharsets.UTF_8)
            );

            outputStream.flush();

            System.out.println(
                    "Message sent: " + message
            );
        }
    }
}
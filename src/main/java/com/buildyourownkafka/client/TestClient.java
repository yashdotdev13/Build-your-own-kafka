package com.buildyourownkafka.client;

import com.buildyourownkafka.protocol.Frame;
import com.buildyourownkafka.protocol.FrameEncoder;

import java.net.Socket;

public class TestClient {

    public static void main(String[] args) throws Exception {

        try (Socket socket = new Socket("localhost", 9092)) {

            System.out.println("Connected to broker.");

            FrameEncoder encoder = new FrameEncoder();

            sendMessage(encoder, socket, "Hello");
            sendMessage(encoder, socket, "Kafka");
            sendMessage(encoder, socket, "From");
            sendMessage(encoder, socket, "Client");

            System.out.println("All frames sent.");
        }
    }

    private static void sendMessage(
            FrameEncoder encoder,
            Socket socket,
            String message) throws Exception {

        Frame frame = new Frame(message);

        encoder.encode(
                frame,
                socket.getOutputStream()
        );

        System.out.println(
                "Sent frame: " + message
        );
    }
}
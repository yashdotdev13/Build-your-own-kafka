package com.buildyourownkafka.client;

import com.buildyourownkafka.protocol.Frame;
import com.buildyourownkafka.protocol.FrameEncoder;

import java.net.Socket;

public class TestClient {

    public static void main(String[] args) throws Exception {

        try (Socket socket = new Socket("localhost", 9092)) {

            System.out.println("Connected to broker.");

            Frame frame =
                    new Frame("Hello Broker");

            FrameEncoder encoder =
                    new FrameEncoder();

            encoder.encode(
                    frame,
                    socket.getOutputStream()
            );

            System.out.println(
                    "Frame sent: " + frame.payloadAsString()
            );
        }
    }
}
package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.Frame;
import com.buildyourownkafka.protocol.FrameDecoder;

import java.io.IOException;
import java.net.Socket;

public class ClientConnection {

    private final Socket socket;

    public ClientConnection(Socket socket) {
        this.socket = socket;
    }

    public void handle() throws IOException {

        FrameDecoder decoder =
                new FrameDecoder();

        Frame frame =
                decoder.decode(socket.getInputStream());

        if (frame == null) {
            System.out.println(
                    "Client disconnected: "
                            + socket.getRemoteSocketAddress()
            );
            return;
        }

        System.out.println(
                "Received frame [" +
                        frame.length() +
                        " bytes]: " +
                        frame.payloadAsString()
        );
    }
}
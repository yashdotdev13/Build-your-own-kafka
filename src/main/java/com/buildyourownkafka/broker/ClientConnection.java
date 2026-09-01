package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestDecoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseEncoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class ClientConnection {

    private final Socket socket;

    public ClientConnection(Socket socket) {
        this.socket = socket;
    }

    public void handle() throws IOException {

        DataInputStream input =
                new DataInputStream(
                        socket.getInputStream()
                );

        DataOutputStream output =
                new DataOutputStream(
                        socket.getOutputStream()
                );

        RequestDecoder requestDecoder =
                new RequestDecoder(input);

        ResponseEncoder responseEncoder =
                new ResponseEncoder(output);

        RequestDispatcher dispatcher =
                new RequestDispatcher();

        while (true) {

            Request request =
                    requestDecoder.decode();

            if (request == null) {
                System.out.println(
                        "Client disconnected: "
                                + socket.getRemoteSocketAddress()
                );
                break;
            }

            System.out.println(
                    "Received request: type=" +
                            request.type() +
                            ", correlationId=" +
                            request.correlationId()
            );

            Response response =
                    dispatcher.dispatch(request);

            responseEncoder.encode(response);

            System.out.println(
                    "Sent response: correlationId=" +
                            response.correlationId()
            );
        }
    }
}
package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestDecoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseEncoder;

import java.io.IOException;
import java.net.Socket;

public class ClientConnection {

    private final Socket socket;

    public ClientConnection(Socket socket) {
        this.socket = socket;
    }

    public void handle() throws IOException {

        RequestDecoder requestDecoder = new RequestDecoder();
        ResponseEncoder responseEncoder = new ResponseEncoder();
        RequestHandler requestHandler = new RequestHandler();

        while (true) {
            Request request = requestDecoder.decode(socket.getInputStream());
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

            Response response = requestHandler.handle(request);


            responseEncoder.encode(
                    response,
                    socket.getOutputStream()
            );

            System.out.println(
                    "Sent response: correlationId=" +
                            response.correlationId()
            );
        }
    }
}
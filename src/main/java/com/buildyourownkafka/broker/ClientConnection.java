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
    private final TopicManager topicManager;
    private final ConsumerOffsetStore consumerOffsetStore;

    public ClientConnection(Socket socket, TopicManager topicManager,
                            ConsumerOffsetStore consumerOffsetStore) {

        this.socket = socket;
        this.topicManager = topicManager;
        this.consumerOffsetStore = consumerOffsetStore;
    }

    public void handle() throws IOException {
        DataInputStream input = new DataInputStream(socket.getInputStream());
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        RequestDecoder requestDecoder = new RequestDecoder(input);
        ResponseEncoder responseEncoder = new ResponseEncoder(output);
        RequestDispatcher dispatcher = new RequestDispatcher(topicManager, consumerOffsetStore);

        while (true) {
            Request request = requestDecoder.decode();
            if (request == null) {
                break;
            }
            System.out.println("Received request: type=" + request.type() + ", correlationId=" + request.correlationId());
            Response response = dispatcher.dispatch(request);
            responseEncoder.encode(response);
        }
        System.out.println("Client disconnected: " + socket.getRemoteSocketAddress());
    }
}
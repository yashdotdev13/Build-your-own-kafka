package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestDecoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseEncoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;

public class ClientConnection {

    private final Socket socket;
    private final TopicManager topicManager;
    private final int socketReadTimeoutMs;
    private final int requestTimeoutMs;
    private final ConsumerOffsetStore consumerOffsetStore;
    private final ConsumerGroupCoordinator consumerGroupCoordinator;

    public ClientConnection(Socket socket, TopicManager topicManager, ConsumerOffsetStore consumerOffsetStore, ConsumerGroupCoordinator consumerGroupCoordinator) {
        this(socket, topicManager, consumerOffsetStore, consumerGroupCoordinator, BrokerNetworkConfig.SOCKET_READ_TIMEOUT_MS, BrokerNetworkConfig.REQUEST_TIMEOUT_MS);
    }

    ClientConnection(Socket socket, TopicManager topicManager, ConsumerOffsetStore consumerOffsetStore, ConsumerGroupCoordinator consumerGroupCoordinator,
                     int socketReadTimeoutMs) {
        this(socket, topicManager, consumerOffsetStore, consumerGroupCoordinator, socketReadTimeoutMs, BrokerNetworkConfig.REQUEST_TIMEOUT_MS);
    }

    ClientConnection(Socket socket, TopicManager topicManager, ConsumerOffsetStore consumerOffsetStore, ConsumerGroupCoordinator consumerGroupCoordinator,
                     int socketReadTimeoutMs, int requestTimeoutMs) {
        if (socketReadTimeoutMs <= 0 || requestTimeoutMs <= 0) {
            throw new IllegalArgumentException("Timeout values must be positive");
        }

        this.socket = socket;
        this.topicManager = topicManager;
        this.consumerOffsetStore = consumerOffsetStore;
        this.consumerGroupCoordinator = consumerGroupCoordinator;
        this.socketReadTimeoutMs = socketReadTimeoutMs;
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public void handle() throws IOException {

        // Wait indefinitely for the first byte of a new request.
        socket.setSoTimeout(0);
        DataInputStream input = new DataInputStream(socket.getInputStream());
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        RequestDecoder requestDecoder = new RequestDecoder(input, socket, requestTimeoutMs, socketReadTimeoutMs);
        ResponseEncoder responseEncoder = new ResponseEncoder(output);
        RequestDispatcher dispatcher = new RequestDispatcher(topicManager, consumerOffsetStore, consumerGroupCoordinator);

        try {
            while (true) {
                Request request = requestDecoder.decode();

                if (request == null) {
                    break;
                }
                System.out.println("Received request: type=" + request.type() + ", correlationId=" + request.correlationId());
                Response response = dispatcher.dispatch(request);
                responseEncoder.encode(response);
            }
        } catch (SocketTimeoutException e) {
            System.err.println("Client read timed out: " + socket.getRemoteSocketAddress() + ", socketReadTimeoutMs=" + socketReadTimeoutMs
                    + ", requestTimeoutMs=" + requestTimeoutMs);
        } finally {
            System.out.println("Client connection closed: " + socket.getRemoteSocketAddress());
        }
    }
}

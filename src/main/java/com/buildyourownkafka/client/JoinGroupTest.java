package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.JoinGroupRequestPayload;
import com.buildyourownkafka.broker.JoinGroupResponsePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestEncoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseDecoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.List;

public class JoinGroupTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== JOIN GROUP END-TO-END TEST ===");
        try (Socket socket = new Socket("localhost", 9092)) {

            System.out.println("Connected to broker.");
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            DataInputStream input = new DataInputStream(socket.getInputStream());
            RequestEncoder requestEncoder = new RequestEncoder(output);
            ResponseDecoder responseDecoder = new ResponseDecoder(input);


            JoinGroupRequestPayload payload = new JoinGroupRequestPayload("orders-group", "consumer-A", 4);
            Request request = new Request(Request.JOIN_GROUP, (short) 1, 200, payload.encode());

            System.out.println("Sending JOIN_GROUP request...");
            requestEncoder.encode(request);
            Response response = responseDecoder.decode();
            if (response.status() != Response.SUCCESS) {
                throw new RuntimeException("JOIN_GROUP failed: " + new String(response.payload()));
            }
            if (response.correlationId() != 200) {
                throw new RuntimeException("Correlation ID mismatch");
            }
            JoinGroupResponsePayload responsePayload = JoinGroupResponsePayload.decode(response.payload());
            System.out.println("Member ID: " + responsePayload.memberId());
            System.out.println("Generation: " + responsePayload.generation());
            System.out.println("Partitions: " + responsePayload.partitions());
            if (!responsePayload.memberId().equals("consumer-A")) {
                throw new RuntimeException("Incorrect member ID");
            }
            if (responsePayload.generation() != 0) {
                throw new RuntimeException("Initial generation should be 0");
            }
            if (!responsePayload.partitions().equals(List.of(0, 1, 2, 3))) {
                throw new RuntimeException("Incorrect partition assignment");
            }
            System.out.println();
            System.out.println("JOIN_GROUP end-to-end " + "test passed successfully!");
        }
    }
}
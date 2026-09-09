package com.buildyourownkafka.client;

import com.buildyourownkafka.protocol.CreateTopicPayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestEncoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseDecoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class TestClient {

    public static void main(String[] args) throws Exception {

        try (Socket socket = new Socket("localhost", 9092)) {

            System.out.println("Connected to broker.");
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            DataInputStream input = new DataInputStream(socket.getInputStream());
            RequestEncoder requestEncoder = new RequestEncoder(output);
            ResponseDecoder responseDecoder = new ResponseDecoder(input);


            Request pingRequest = new Request(Request.PING, (short) 1, 42, new byte[0]);
            requestEncoder.encode(pingRequest);
            Response pingResponse = responseDecoder.decode();

            System.out.println();
            System.out.println("PING RESPONSE");
            System.out.println("Correlation ID: " + pingResponse.correlationId());
            System.out.println("Status: " + pingResponse.status());
            System.out.println("Payload: " + new String(pingResponse.payload(), StandardCharsets.UTF_8));


            CreateTopicPayload createTopicPayload = new CreateTopicPayload("orders", 3);

            System.out.println();
            System.out.println("Sending CREATE_TOPIC: " + createTopicPayload.topicName() + ", partitions=" + createTopicPayload.partitionCount());

            Request createTopicRequest = new Request(Request.CREATE_TOPIC, (short) 1, 100, createTopicPayload.encode());
            requestEncoder.encode(createTopicRequest);
            Response createTopicResponse = responseDecoder.decode();

            System.out.println();
            System.out.println("CREATE_TOPIC RESPONSE");
            System.out.println("Correlation ID: " + createTopicResponse.correlationId());
            System.out.println("Status: " + createTopicResponse.status());
            System.out.println("Payload: " + new String(createTopicResponse.payload(), StandardCharsets.UTF_8));
        }
    }
}
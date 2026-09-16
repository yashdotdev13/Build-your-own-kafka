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

public class MultiConsumerJoinGroupTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== MULTI CONSUMER JOIN GROUP TEST ===");
        try (Socket consumerA = new Socket("localhost", 9092);
             Socket consumerB = new Socket("localhost", 9092)) {

            System.out.println("Both consumers connected.");
            DataOutputStream outputA = new DataOutputStream(consumerA.getOutputStream());

            DataInputStream inputA = new DataInputStream(consumerA.getInputStream());

            DataOutputStream outputB = new DataOutputStream(consumerB.getOutputStream());
            DataInputStream inputB = new DataInputStream(consumerB.getInputStream());
            RequestEncoder encoderA = new RequestEncoder(outputA);
            ResponseDecoder decoderA = new ResponseDecoder(inputA);
            RequestEncoder encoderB = new RequestEncoder(outputB);
            ResponseDecoder decoderB = new ResponseDecoder(inputB);

            JoinGroupRequestPayload payloadA = new JoinGroupRequestPayload("orders-group", "consumer-A", 4);
            Request requestA = new Request(Request.JOIN_GROUP, (short) 1, 300, payloadA.encode());
            encoderA.encode(requestA);
            Response responseA = decoderA.decode();

            if (responseA.status() != Response.SUCCESS) {
                throw new RuntimeException("Consumer-A JOIN_GROUP failed");
            }
            JoinGroupResponsePayload resultA = JoinGroupResponsePayload.decode(responseA.payload());
            System.out.println("Consumer-A initial assignment: " + resultA.partitions());
            if (!resultA.partitions().equals(List.of(0, 1, 2, 3))) {
                throw new RuntimeException("Consumer-A should initially own all partitions");
            }
            JoinGroupRequestPayload payloadB = new JoinGroupRequestPayload("orders-group", "consumer-B", 4);
            Request requestB = new Request(Request.JOIN_GROUP, (short) 1, 301, payloadB.encode());
            encoderB.encode(requestB);
            Response responseB = decoderB.decode();
            if (responseB.status() != Response.SUCCESS) {
                throw new RuntimeException("Consumer-B JOIN_GROUP failed");
            }
            JoinGroupResponsePayload resultB = JoinGroupResponsePayload.decode(responseB.payload());
            System.out.println("Consumer-B assignment: " + resultB.partitions());

            if (!resultB.partitions().equals(List.of(1, 3))) {
                throw new RuntimeException("Consumer-B should receive [1, 3]");
            }
            System.out.println();
            System.out.println("Consumer-A old assignment: " + resultA.partitions());
            System.out.println("Consumer-B new assignment: " + resultB.partitions());
            System.out.println();
            System.out.println("Multi-consumer JOIN_GROUP " + "verified successfully!");
        }
    }
}
package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.JoinGroupRequestPayload;
import com.buildyourownkafka.broker.SyncGroupRequestPayload;
import com.buildyourownkafka.broker.SyncGroupResponsePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestEncoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseDecoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class SyncGroupTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== SYNC GROUP END-TO-END TEST ===");

        try (Socket socket = new Socket("localhost", 9092)) {
            System.out.println("Connected to broker.");
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            DataInputStream input = new DataInputStream(socket.getInputStream());
            RequestEncoder requestEncoder = new RequestEncoder(output);
            ResponseDecoder responseDecoder = new ResponseDecoder(input);

            JoinGroupRequestPayload joinA = new JoinGroupRequestPayload("sync-test-group", "consumer-A", 4);
            Request joinRequestA = new Request(Request.JOIN_GROUP, (short) 1, 1, joinA.encode());
            System.out.println("Sending JOIN_GROUP for consumer-A...");
            requestEncoder.encode(joinRequestA);
            Response joinResponseA = responseDecoder.decode();
            if (joinResponseA.status() != Response.SUCCESS) {
                String errorMessage = new String(joinResponseA.payload());
                System.err.println("Consumer-A JOIN_GROUP failed: " + errorMessage);
                throw new AssertionError("Consumer-A JOIN_GROUP failed");
            }
            System.out.println("Consumer-A JOIN_GROUP successful.");
            JoinGroupRequestPayload joinB = new JoinGroupRequestPayload("sync-test-group", "consumer-B", 4);
            Request joinRequestB = new Request(Request.JOIN_GROUP, (short) 1, 2, joinB.encode());
            System.out.println("Sending JOIN_GROUP for consumer-B...");
            requestEncoder.encode(joinRequestB);
            Response joinResponseB = responseDecoder.decode();
            if (joinResponseB.status() != Response.SUCCESS) {
                String errorMessage = new String(joinResponseB.payload());
                System.err.println("Consumer-B JOIN_GROUP failed: " + errorMessage);
                throw new AssertionError("Consumer-B JOIN_GROUP failed");
            }
            System.out.println("Consumer-B JOIN_GROUP successful.");

            SyncGroupRequestPayload syncA = new SyncGroupRequestPayload("sync-test-group", "consumer-A", 0);
            Request syncRequestA = new Request(Request.SYNC_GROUP, (short) 1, 3, syncA.encode());
            System.out.println("Sending SYNC_GROUP for consumer-A...");
            requestEncoder.encode(syncRequestA);
            Response syncResponseA = responseDecoder.decode();
            if (syncResponseA.status() != Response.SUCCESS) {
                String errorMessage = new String(syncResponseA.payload());
                System.err.println("Consumer-A SYNC_GROUP failed: " + errorMessage);
                throw new AssertionError("Consumer-A SYNC_GROUP failed");
            }
            SyncGroupResponsePayload responsePayload = SyncGroupResponsePayload.decode(syncResponseA.payload());
            System.out.println("Consumer-A member ID: " + responsePayload.memberId());
            System.out.println("Consumer-A generation: " + responsePayload.generation());
            System.out.println("Consumer-A current partitions: " + responsePayload.partitions());
            if (!responsePayload.memberId().equals("consumer-A")) {

                throw new AssertionError("Member ID mismatch");
            }
            if (responsePayload.generation() != 0) {
                throw new AssertionError("Generation mismatch");
            }
            if (!responsePayload.partitions().equals(java.util.List.of(0, 2))) {
                throw new AssertionError("Consumer-A current assignment mismatch");
            }
            if (syncResponseA.correlationId() != 3) {
                throw new AssertionError("Correlation ID mismatch");
            }
            System.out.println();
            System.out.println("SYNC_GROUP end-to-end test " + "passed successfully!");
        }
    }
}
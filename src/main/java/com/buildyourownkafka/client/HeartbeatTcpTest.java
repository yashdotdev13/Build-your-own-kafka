package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.HeartbeatRequestPayload;
import com.buildyourownkafka.broker.HeartbeatResponsePayload;
import com.buildyourownkafka.broker.JoinGroupRequestPayload;
import com.buildyourownkafka.broker.JoinGroupResponsePayload;
import com.buildyourownkafka.broker.SyncGroupRequestPayload;
import com.buildyourownkafka.broker.SyncGroupResponsePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestEncoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseDecoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class HeartbeatTcpTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== HEARTBEAT TCP TEST ===");

        String host = "localhost";
        int port = 9092;

        String groupId = "heartbeat-group";
        String memberId = "consumer-heartbeat-test";
        int partitionCount = 4;

        try (
                Socket socket = new Socket(host, port);
                DataInputStream input =
                        new DataInputStream(socket.getInputStream());
                DataOutputStream output =
                        new DataOutputStream(socket.getOutputStream())
        ) {

            System.out.println(
                    "Connected to broker at "
                            + host + ":" + port
            );

            RequestEncoder requestEncoder =
                    new RequestEncoder(output);

            ResponseDecoder responseDecoder =
                    new ResponseDecoder(input);

            int correlationId = 500;

            // ---------------------------------------------------------
            // 1. JOIN_GROUP
            // ---------------------------------------------------------

            System.out.println();
            System.out.println("Sending JOIN_GROUP...");

            JoinGroupRequestPayload joinPayload =
                    new JoinGroupRequestPayload(
                            groupId,
                            memberId,
                            partitionCount
                    );

            Request joinRequest =
                    new Request(
                            Request.JOIN_GROUP,
                            (short) 1,
                            correlationId++,
                            joinPayload.encode()
                    );

            requestEncoder.encode(joinRequest);

            Response joinResponse =
                    responseDecoder.decode();

            if (joinResponse.status() != Response.SUCCESS) {
                throw new AssertionError(
                        "JOIN_GROUP failed: "
                                + new String(joinResponse.payload())
                );
            }

            JoinGroupResponsePayload joinResult =
                    JoinGroupResponsePayload.decode(
                            joinResponse.payload()
                    );

            String assignedMemberId =
                    joinResult.memberId();

            int generation =
                    joinResult.generation();

            System.out.println(
                    "JOIN_GROUP successful."
            );

            System.out.println(
                    "Member ID: "
                            + assignedMemberId
            );

            System.out.println(
                    "Generation: "
                            + generation
            );

            // ---------------------------------------------------------
            // 2. SYNC_GROUP
            // ---------------------------------------------------------

            System.out.println();
            System.out.println("Sending SYNC_GROUP...");

            SyncGroupRequestPayload syncPayload =
                    new SyncGroupRequestPayload(
                            groupId,
                            assignedMemberId,
                            generation
                    );

            Request syncRequest =
                    new Request(
                            Request.SYNC_GROUP,
                            (short) 1,
                            correlationId++,
                            syncPayload.encode()
                    );

            requestEncoder.encode(syncRequest);

            Response syncResponse =
                    responseDecoder.decode();

            if (syncResponse.status() != Response.SUCCESS) {
                throw new AssertionError(
                        "SYNC_GROUP failed: "
                                + new String(syncResponse.payload())
                );
            }

            SyncGroupResponsePayload syncResult =
                    SyncGroupResponsePayload.decode(
                            syncResponse.payload()
                    );

            System.out.println(
                    "SYNC_GROUP successful."
            );

            System.out.println(
                    "Assigned partitions: "
                            + syncResult.partitions()
            );

            // ---------------------------------------------------------
            // 3. HEARTBEAT
            // ---------------------------------------------------------

            System.out.println();
            System.out.println("Sending HEARTBEAT...");

            HeartbeatRequestPayload heartbeatPayload =
                    new HeartbeatRequestPayload(
                            groupId,
                            assignedMemberId,
                            generation
                    );

            Request heartbeatRequest =
                    new Request(
                            Request.HEARTBEAT,
                            (short) 1,
                            correlationId++,
                            heartbeatPayload.encode()
                    );

            requestEncoder.encode(heartbeatRequest);

            Response heartbeatResponse =
                    responseDecoder.decode();

            System.out.println(
                    "Received HEARTBEAT response."
            );

            System.out.println(
                    "Correlation ID: "
                            + heartbeatResponse.correlationId()
            );

            System.out.println(
                    "Response status: "
                            + heartbeatResponse.status()
            );

            if (heartbeatResponse.status() != Response.SUCCESS) {
                throw new AssertionError(
                        "HEARTBEAT failed: "
                                + new String(
                                heartbeatResponse.payload()
                        )
                );
            }

            HeartbeatResponsePayload responsePayload =
                    HeartbeatResponsePayload.decode(
                            heartbeatResponse.payload()
                    );

            System.out.println(
                    "Response member: "
                            + responsePayload.memberId()
            );

            System.out.println(
                    "Response generation: "
                            + responsePayload.generation()
            );

            if (!assignedMemberId.equals(
                    responsePayload.memberId()
            )) {
                throw new AssertionError(
                        "Member ID mismatch"
                );
            }

            if (generation != responsePayload.generation()) {
                throw new AssertionError(
                        "Generation mismatch"
                );
            }

            System.out.println();
            System.out.println(
                    "JOIN_GROUP → SYNC_GROUP → HEARTBEAT VERIFIED!"
            );
        }
    }
}
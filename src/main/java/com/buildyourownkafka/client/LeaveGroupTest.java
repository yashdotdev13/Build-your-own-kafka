package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.LeaveGroupRequestPayload;
import com.buildyourownkafka.broker.LeaveGroupResponsePayload;
import com.buildyourownkafka.broker.JoinGroupRequestPayload;
import com.buildyourownkafka.broker.JoinGroupResponsePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestEncoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseDecoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class LeaveGroupTest {

    private static final String HOST = "localhost";
    private static final int PORT = 9092;

    public static void main(String[] args) throws Exception {

        System.out.println(
                "=== LEAVE GROUP TCP INTEGRATION TEST ==="
        );

        /*
         * Create two separate consumer connections.
         */

        try (
                Socket consumerA =
                        new Socket(HOST, PORT);

                Socket consumerB =
                        new Socket(HOST, PORT)
        ) {

            System.out.println(
                    "Both consumers connected."
            );

            DataOutputStream outputA =
                    new DataOutputStream(
                            consumerA.getOutputStream()
                    );

            DataInputStream inputA =
                    new DataInputStream(
                            consumerA.getInputStream()
                    );

            DataOutputStream outputB =
                    new DataOutputStream(
                            consumerB.getOutputStream()
                    );

            DataInputStream inputB =
                    new DataInputStream(
                            consumerB.getInputStream()
                    );

            /*
             * Consumer-A JOIN_GROUP
             */

            JoinGroupRequestPayload joinPayloadA =
                    new JoinGroupRequestPayload(
                            "orders-group",
                            "consumer-A",
                            4
                    );

            Request joinRequestA =
                    new Request(
                            Request.JOIN_GROUP,
                            (short) 1,
                            1,
                            joinPayloadA.encode()
                    );

            RequestEncoder encoderA =
                    new RequestEncoder(outputA);

            encoderA.encode(joinRequestA);

            Response joinResponseA =
                    new ResponseDecoder(inputA)
                            .decode();

            if (joinResponseA.status()
                    != Response.SUCCESS) {

                throw new AssertionError(
                        "Consumer-A JOIN_GROUP failed"
                );
            }

            JoinGroupResponsePayload joinResultA =
                    JoinGroupResponsePayload.decode(
                            joinResponseA.payload()
                    );

            System.out.println(
                    "Consumer-A JOIN assignment: "
                            + joinResultA.partitions()
            );

            /*
             * Consumer-B JOIN_GROUP
             */

            JoinGroupRequestPayload joinPayloadB =
                    new JoinGroupRequestPayload(
                            "orders-group",
                            "consumer-B",
                            4
                    );

            Request joinRequestB =
                    new Request(
                            Request.JOIN_GROUP,
                            (short) 1,
                            2,
                            joinPayloadB.encode()
                    );

            RequestEncoder encoderB =
                    new RequestEncoder(outputB);

            encoderB.encode(joinRequestB);

            Response joinResponseB =
                    new ResponseDecoder(inputB)
                            .decode();

            if (joinResponseB.status()
                    != Response.SUCCESS) {

                throw new AssertionError(
                        "Consumer-B JOIN_GROUP failed"
                );
            }

            JoinGroupResponsePayload joinResultB =
                    JoinGroupResponsePayload.decode(
                            joinResponseB.payload()
                    );

            System.out.println(
                    "Consumer-B JOIN assignment: "
                            + joinResultB.partitions()
            );

            /*
             * Consumer-A LEAVE_GROUP
             */

            LeaveGroupRequestPayload leavePayload =
                    new LeaveGroupRequestPayload(
                            "orders-group",
                            "consumer-A",
                            4
                    );

            Request leaveRequest =
                    new Request(
                            Request.LEAVE_GROUP,
                            (short) 1,
                            3,
                            leavePayload.encode()
                    );

            encoderA.encode(leaveRequest);

            Response leaveResponse =
                    new ResponseDecoder(inputA)
                            .decode();

            /*
             * Verify response.
             */

            if (leaveResponse.status()
                    != Response.SUCCESS) {

                throw new AssertionError(
                        "Consumer-A LEAVE_GROUP failed"
                );
            }

            if (leaveResponse.correlationId() != 3) {

                throw new AssertionError(
                        "LEAVE_GROUP correlation ID mismatch"
                );
            }

            LeaveGroupResponsePayload leaveResult =
                    LeaveGroupResponsePayload.decode(
                            leaveResponse.payload()
                    );

            System.out.println(
                    "Consumer-A left group: "
                            + leaveResult.memberId()
            );

            if (!leaveResult.memberId()
                    .equals("consumer-A")) {

                throw new AssertionError(
                        "Incorrect member in LEAVE_GROUP response"
                );
            }

            /*
             * Consumer-B should still be connected.
             *
             * Send SYNC_GROUP to verify that the
             * remaining member receives the new assignment.
             */

            /*
             * The current coordinator implementation uses
             * generation 0 for the group members.
             */

            com.buildyourownkafka.broker.SyncGroupRequestPayload syncPayload =
                    new com.buildyourownkafka.broker.SyncGroupRequestPayload(
                            "orders-group",
                            "consumer-B",
                            0
                    );

            Request syncRequest =
                    new Request(
                            Request.SYNC_GROUP,
                            (short) 1,
                            4,
                            syncPayload.encode()
                    );

            encoderB.encode(syncRequest);

            Response syncResponse =
                    new ResponseDecoder(inputB)
                            .decode();

            if (syncResponse.status()
                    != Response.SUCCESS) {

                throw new AssertionError(
                        "Consumer-B SYNC_GROUP failed after leave"
                );
            }

            com.buildyourownkafka.broker.SyncGroupResponsePayload syncResult =
                    com.buildyourownkafka.broker.SyncGroupResponsePayload.decode(
                            syncResponse.payload()
                    );

            System.out.println(
                    "Consumer-B assignment after leave: "
                            + syncResult.partitions()
            );

            /*
             * After consumer-A leaves, consumer-B should
             * receive all four partitions.
             */

            if (!syncResult.partitions()
                    .equals(java.util.List.of(0, 1, 2, 3))) {

                throw new AssertionError(
                        "Consumer-B did not receive all partitions"
                );
            }

            System.out.println();

            System.out.println(
                    "LEAVE_GROUP TCP integration "
                            + "verified successfully!"
            );
        }
    }
}
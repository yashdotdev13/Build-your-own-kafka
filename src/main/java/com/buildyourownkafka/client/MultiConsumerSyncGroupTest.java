package com.buildyourownkafka.client;

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
import java.util.List;

public class MultiConsumerSyncGroupTest {

    public static void main(String[] args) throws Exception {

        System.out.println(
                "=== MULTI CONSUMER SYNC GROUP TEST ==="
        );

        try (
                Socket socketA =
                        new Socket("localhost", 9092);

                Socket socketB =
                        new Socket("localhost", 9092)
        ) {

            System.out.println(
                    "Both consumers connected."
            );

            DataOutputStream outputA =
                    new DataOutputStream(
                            socketA.getOutputStream()
                    );

            DataInputStream inputA =
                    new DataInputStream(
                            socketA.getInputStream()
                    );

            DataOutputStream outputB =
                    new DataOutputStream(
                            socketB.getOutputStream()
                    );

            DataInputStream inputB =
                    new DataInputStream(
                            socketB.getInputStream()
                    );

            RequestEncoder encoderA =
                    new RequestEncoder(outputA);

            ResponseDecoder decoderA =
                    new ResponseDecoder(inputA);

            RequestEncoder encoderB =
                    new RequestEncoder(outputB);

            ResponseDecoder decoderB =
                    new ResponseDecoder(inputB);

            /*
             * =====================================================
             * Consumer-A JOIN_GROUP
             * =====================================================
             */

            JoinGroupRequestPayload joinPayloadA =
                    new JoinGroupRequestPayload(
                            "multi-sync-group",
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

            encoderA.encode(joinRequestA);

            Response joinResponseA =
                    decoderA.decode();

            if (joinResponseA.status()
                    != Response.SUCCESS) {

                throw new AssertionError(
                        "Consumer-A JOIN_GROUP failed: "
                                + new String(
                                joinResponseA.payload()
                        )
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
             * =====================================================
             * Consumer-B JOIN_GROUP
             * =====================================================
             */

            JoinGroupRequestPayload joinPayloadB =
                    new JoinGroupRequestPayload(
                            "multi-sync-group",
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

            encoderB.encode(joinRequestB);

            Response joinResponseB =
                    decoderB.decode();

            if (joinResponseB.status()
                    != Response.SUCCESS) {

                throw new AssertionError(
                        "Consumer-B JOIN_GROUP failed: "
                                + new String(
                                joinResponseB.payload()
                        )
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
             * =====================================================
             * Consumer-A SYNC_GROUP
             * =====================================================
             */

            SyncGroupRequestPayload syncPayloadA =
                    new SyncGroupRequestPayload(
                            "multi-sync-group",
                            "consumer-A",
                            joinResultA.generation()
                    );

            Request syncRequestA =
                    new Request(
                            Request.SYNC_GROUP,
                            (short) 1,
                            3,
                            syncPayloadA.encode()
                    );

            encoderA.encode(syncRequestA);

            Response syncResponseA =
                    decoderA.decode();

            if (syncResponseA.status()
                    != Response.SUCCESS) {

                throw new AssertionError(
                        "Consumer-A SYNC_GROUP failed: "
                                + new String(
                                syncResponseA.payload()
                        )
                );
            }

            SyncGroupResponsePayload syncResultA =
                    SyncGroupResponsePayload.decode(
                            syncResponseA.payload()
                    );

            System.out.println(
                    "Consumer-A SYNC assignment: "
                            + syncResultA.partitions()
            );

            /*
             * =====================================================
             * Consumer-B SYNC_GROUP
             * =====================================================
             */

            SyncGroupRequestPayload syncPayloadB =
                    new SyncGroupRequestPayload(
                            "multi-sync-group",
                            "consumer-B",
                            joinResultB.generation()
                    );

            Request syncRequestB =
                    new Request(
                            Request.SYNC_GROUP,
                            (short) 1,
                            4,
                            syncPayloadB.encode()
                    );

            encoderB.encode(syncRequestB);

            Response syncResponseB =
                    decoderB.decode();

            if (syncResponseB.status()
                    != Response.SUCCESS) {

                throw new AssertionError(
                        "Consumer-B SYNC_GROUP failed: "
                                + new String(
                                syncResponseB.payload()
                        )
                );
            }

            SyncGroupResponsePayload syncResultB =
                    SyncGroupResponsePayload.decode(
                            syncResponseB.payload()
                    );

            System.out.println(
                    "Consumer-B SYNC assignment: "
                            + syncResultB.partitions()
            );

            /*
             * =====================================================
             * Validate assignments
             * =====================================================
             */

            if (!syncResultA.partitions()
                    .equals(List.of(0, 2))) {

                throw new AssertionError(
                        "Consumer-A SYNC assignment mismatch"
                );
            }

            if (!syncResultB.partitions()
                    .equals(List.of(1, 3))) {

                throw new AssertionError(
                        "Consumer-B SYNC assignment mismatch"
                );
            }

            if (!syncResultA.memberId()
                    .equals("consumer-A")) {

                throw new AssertionError(
                        "Consumer-A member ID mismatch"
                );
            }

            if (!syncResultB.memberId()
                    .equals("consumer-B")) {

                throw new AssertionError(
                        "Consumer-B member ID mismatch"
                );
            }

            System.out.println();

            System.out.println(
                    "Multi-consumer SYNC_GROUP "
                            + "verified successfully!"
            );
        }
    }
}
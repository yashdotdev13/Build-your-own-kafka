package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.SyncGroupResponsePayload;

import java.util.List;

public class SyncGroupResponsePayloadTest {

    public static void main(String[] args) throws Exception {

        System.out.println(
                "=== SYNC GROUP RESPONSE PAYLOAD TEST ==="
        );

        SyncGroupResponsePayload original =
                new SyncGroupResponsePayload(
                        "consumer-A",
                        0,
                        List.of(0, 2)
                );

        byte[] encoded = original.encode();

        System.out.println(
                "Encoded payload size: "
                        + encoded.length
        );

        SyncGroupResponsePayload decoded =
                SyncGroupResponsePayload.decode(encoded);

        System.out.println(
                "Member ID: "
                        + decoded.memberId()
        );

        System.out.println(
                "Generation: "
                        + decoded.generation()
        );

        System.out.println(
                "Partitions: "
                        + decoded.partitions()
        );

        if (!decoded.memberId().equals("consumer-A")) {
            throw new AssertionError(
                    "Member ID mismatch"
            );
        }

        if (decoded.generation() != 0) {
            throw new AssertionError(
                    "Generation mismatch"
            );
        }

        if (!decoded.partitions().equals(List.of(0, 2))) {
            throw new AssertionError(
                    "Partitions mismatch"
            );
        }

        System.out.println();

        System.out.println(
                "SYNC_GROUP response payload "
                        + "verified successfully!"
        );
    }
}
package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.SyncGroupRequestPayload;

public class SyncGroupRequestPayloadTest {

    public static void main(String[] args) throws Exception {

        System.out.println(
                "=== SYNC GROUP REQUEST PAYLOAD TEST ==="
        );

        SyncGroupRequestPayload original =
                new SyncGroupRequestPayload(
                        "orders-group",
                        "consumer-A",
                        0
                );

        byte[] encoded = original.encode();

        System.out.println(
                "Encoded payload size: "
                        + encoded.length
        );

        SyncGroupRequestPayload decoded =
                SyncGroupRequestPayload.decode(encoded);

        System.out.println(
                "Group ID: "
                        + decoded.groupId()
        );

        System.out.println(
                "Member ID: "
                        + decoded.memberId()
        );

        System.out.println(
                "Generation: "
                        + decoded.generation()
        );

        if (!decoded.groupId().equals("orders-group")) {
            throw new AssertionError(
                    "Group ID mismatch"
            );
        }

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

        System.out.println();
        System.out.println(
                "SYNC_GROUP request payload "
                        + "verified successfully!"
        );
    }
}
package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.HeartbeatRequestPayload;

public class HeartbeatRequestPayloadTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== HEARTBEAT REQUEST PAYLOAD TEST ===");

        HeartbeatRequestPayload original =
                new HeartbeatRequestPayload(
                        "orders-group",
                        "consumer-A",
                        5
                );

        byte[] encoded = original.encode();

        System.out.println("Encoded payload size: " + encoded.length);

        HeartbeatRequestPayload decoded =
                HeartbeatRequestPayload.decode(encoded);

        System.out.println("Decoded group: " + decoded.groupId());
        System.out.println("Decoded member: " + decoded.memberId());
        System.out.println("Decoded generation: " + decoded.generation());

        if (!original.groupId().equals(decoded.groupId())) {
            throw new AssertionError("Group ID mismatch");
        }

        if (!original.memberId().equals(decoded.memberId())) {
            throw new AssertionError("Member ID mismatch");
        }

        if (original.generation() != decoded.generation()) {
            throw new AssertionError("Generation mismatch");
        }

        System.out.println("HEARTBEAT REQUEST PAYLOAD VERIFIED!");
    }
}
package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.JoinGroupResponsePayload;

import java.util.Arrays;
import java.util.List;

public class JoinGroupResponsePayloadTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== JOIN GROUP RESPONSE PAYLOAD TEST ===");

        JoinGroupResponsePayload original = new JoinGroupResponsePayload("consumer-A", 1, List.of(0, 2));
        byte[] encoded = original.encode();
        System.out.println("Encoded payload size: " + encoded.length);
        JoinGroupResponsePayload decoded = JoinGroupResponsePayload.decode(encoded);
        System.out.println("Member ID: " + decoded.memberId());
        System.out.println("Generation: " + decoded.generation());
        System.out.println("Partitions: " + decoded.partitions());

        if (!decoded.memberId().equals(original.memberId())) {
            throw new RuntimeException("Member ID mismatch");
        }
        if (decoded.generation() != original.generation()) {
            throw new RuntimeException("Generation mismatch");
        }
        if (!decoded.partitions().equals(original.partitions())) {
            throw new RuntimeException("Partitions mismatch");
        }
        byte[] encodedAgain = decoded.encode();
        if (!Arrays.equals(encoded, encodedAgain)) {
            throw new RuntimeException("Encoding is not deterministic");
        }
        System.out.println();
        System.out.println("JOIN_GROUP response payload " + "verified successfully!");
    }
}
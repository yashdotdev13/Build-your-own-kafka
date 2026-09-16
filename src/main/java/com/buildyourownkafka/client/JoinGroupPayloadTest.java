package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.JoinGroupRequestPayload;

import java.util.Arrays;

public class JoinGroupPayloadTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== JOIN GROUP PAYLOAD TEST ===");
        JoinGroupRequestPayload original = new JoinGroupRequestPayload("orders-group", "consumer-A", 4);
        byte[] encoded = original.encode();

        System.out.println("Encoded payload size: " + encoded.length);
        JoinGroupRequestPayload decoded = JoinGroupRequestPayload.decode(encoded);
        System.out.println("Group ID: " + decoded.groupId());
        System.out.println("Member ID: " + decoded.memberId());
        System.out.println("Partition count: " + decoded.partitionCount());
        if (!decoded.groupId().equals(original.groupId())) {
            throw new RuntimeException("Group ID mismatch");
        }
        if (!decoded.memberId().equals(original.memberId())) {
            throw new RuntimeException("Member ID mismatch");
        }
        if (decoded.partitionCount() != original.partitionCount()) {
            throw new RuntimeException("Partition count mismatch");
        }
        byte[] encodedAgain = decoded.encode();
        if (!Arrays.equals(encoded, encodedAgain)) {
            throw new RuntimeException("Encoding is not deterministic");
        }
        System.out.println();
        System.out.println("JOIN_GROUP request payload " + "verified successfully!");
    }
}
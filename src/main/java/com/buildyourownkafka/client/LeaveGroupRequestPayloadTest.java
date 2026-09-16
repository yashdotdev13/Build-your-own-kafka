package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.LeaveGroupRequestPayload;

public class LeaveGroupRequestPayloadTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== LEAVE GROUP REQUEST PAYLOAD TEST ===");
        LeaveGroupRequestPayload original = new LeaveGroupRequestPayload("orders-group", "consumer-A", 4);
        byte[] encoded = original.encode();
        System.out.println("Encoded payload size: " + encoded.length);
        LeaveGroupRequestPayload decoded = LeaveGroupRequestPayload.decode(encoded);
        System.out.println("Group ID: " + decoded.groupId());
        System.out.println("Member ID: " + decoded.memberId());
        System.out.println("Partition count: " + decoded.partitionCount());
        if (!decoded.groupId().equals("orders-group")) {
            throw new AssertionError("Group ID mismatch");
        }
        if (!decoded.memberId().equals("consumer-A")) {
            throw new AssertionError("Member ID mismatch");
        }
        if (decoded.partitionCount() != 4) {
            throw new AssertionError("Partition count mismatch");
        }
        System.out.println();
        System.out.println("LEAVE_GROUP request payload " + "verified successfully!");
    }
}
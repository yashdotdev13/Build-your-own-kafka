package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.LeaveGroupResponsePayload;

public class LeaveGroupResponsePayloadTest {

    public static void main(String[] args) throws Exception {

        System.out.println(
                "=== LEAVE GROUP RESPONSE PAYLOAD TEST ==="
        );

        LeaveGroupResponsePayload original =
                new LeaveGroupResponsePayload(
                        "consumer-A"
                );

        byte[] encoded =
                original.encode();

        System.out.println(
                "Encoded payload size: "
                        + encoded.length
        );

        LeaveGroupResponsePayload decoded =
                LeaveGroupResponsePayload.decode(
                        encoded
                );

        System.out.println(
                "Member ID: "
                        + decoded.memberId()
        );

        if (!decoded.memberId()
                .equals("consumer-A")) {

            throw new AssertionError(
                    "Member ID mismatch"
            );
        }

        System.out.println();

        System.out.println(
                "LEAVE_GROUP response payload "
                        + "verified successfully!"
        );
    }
}
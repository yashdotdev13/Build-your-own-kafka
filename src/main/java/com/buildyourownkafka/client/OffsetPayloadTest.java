package com.buildyourownkafka.client;

import com.buildyourownkafka.protocol.CommitOffsetPayload;
import com.buildyourownkafka.protocol.FetchOffsetPayload;

public class OffsetPayloadTest {

    public static void main(String[] args) {

        System.out.println(
                "=== OFFSET PAYLOAD TEST ==="
        );

        CommitOffsetPayload commitPayload =
                new CommitOffsetPayload(
                        "payment-service",
                        "orders",
                        1,
                        4
                );

        byte[] commitBytes =
                commitPayload.encode();

        CommitOffsetPayload decodedCommit =
                CommitOffsetPayload.decode(
                        commitBytes
                );

        System.out.println(
                "COMMIT_OFFSET"
        );

        System.out.println(
                "Group: "
                        + decodedCommit.groupId()
        );

        System.out.println(
                "Topic: "
                        + decodedCommit.topic()
        );

        System.out.println(
                "Partition: "
                        + decodedCommit.partition()
        );

        System.out.println(
                "Offset: "
                        + decodedCommit.offset()
        );

        System.out.println();

        FetchOffsetPayload fetchPayload =
                new FetchOffsetPayload(
                        "payment-service",
                        "orders",
                        1
                );

        byte[] fetchBytes =
                fetchPayload.encode();

        FetchOffsetPayload decodedFetch =
                FetchOffsetPayload.decode(
                        fetchBytes
                );

        System.out.println(
                "FETCH_OFFSET"
        );

        System.out.println(
                "Group: "
                        + decodedFetch.groupId()
        );

        System.out.println(
                "Topic: "
                        + decodedFetch.topic()
        );

        System.out.println(
                "Partition: "
                        + decodedFetch.partition()
        );
    }
}
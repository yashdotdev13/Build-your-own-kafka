package com.buildyourownkafka.client;

import com.buildyourownkafka.protocol.ProduceBatchPayload;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ProduceBatchPayloadTest {

    public static void main(String[] args) {

        ProduceBatchPayload original =
                new ProduceBatchPayload(
                        "orders",
                        1,
                        List.of(
                                "order-123".getBytes(StandardCharsets.UTF_8),
                                "order-456".getBytes(StandardCharsets.UTF_8),
                                "order-789".getBytes(StandardCharsets.UTF_8)
                        )
                );

        byte[] encoded = original.encode();

        ProduceBatchPayload decoded =
                ProduceBatchPayload.decode(encoded);

        System.out.println("Topic: " + decoded.topicName());
        System.out.println("Partition: " + decoded.partitionId());
        System.out.println("Record count: " + decoded.records().size());

        for (byte[] record : decoded.records()) {
            System.out.println(
                    "Record: " + new String(record, StandardCharsets.UTF_8)
            );
        }
    }
}
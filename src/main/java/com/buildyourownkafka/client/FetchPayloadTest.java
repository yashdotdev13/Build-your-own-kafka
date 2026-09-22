package com.buildyourownkafka.client;

import com.buildyourownkafka.protocol.FetchPayload;

public class FetchPayloadTest {

    public static void main(String[] args) {

        FetchPayload payload =
                new FetchPayload("orders", 0, 0, 10);

        byte[] encoded = payload.encode();

        FetchPayload decoded =
                FetchPayload.decode(encoded);

        System.out.println("Topic: " + decoded.topicName());
        System.out.println("Partition: " + decoded.partitionId());
        System.out.println("Offset: " + decoded.offset());
        System.out.println("Max records: " + decoded.maxRecords());

        System.out.println("FETCH PAYLOAD TEST PASSED!");
    }
}
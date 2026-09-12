package com.buildyourownkafka.client;

import com.buildyourownkafka.protocol.FetchPayload;

public class FetchPayloadTest {

    public static void main(String[] args) {

        FetchPayload original = new FetchPayload("orders", 1, 42);
        byte[] encoded = original.encode();

        System.out.println("Encoded payload size: " + encoded.length);
        FetchPayload decoded = FetchPayload.decode(encoded);

        System.out.println("Topic: " + decoded.topicName());
        System.out.println("Partition: " + decoded.partitionId());
        System.out.println("Offset: " + decoded.offset());
    }
}
package com.buildyourownkafka.client;

import com.buildyourownkafka.protocol.ProducePayload;
import java.nio.charset.StandardCharsets;

public class ProducePayloadTest {

    public static void main(String[] args) {

        ProducePayload original = new ProducePayload("orders", 1, "order-123".getBytes(StandardCharsets.UTF_8));
        byte[] encoded = original.encode();
        System.out.println("Encoded payload size: " + encoded.length);
        ProducePayload decoded = ProducePayload.decode(encoded);
        System.out.println("Topic: " + decoded.topicName());
        System.out.println("Partition: " + decoded.partitionId());
        System.out.println("Value: " + new String(decoded.value(), StandardCharsets.UTF_8));
    }
}
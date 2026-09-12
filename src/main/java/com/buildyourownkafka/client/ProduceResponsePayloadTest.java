package com.buildyourownkafka.client;

import com.buildyourownkafka.protocol.ProduceResponsePayload;

public class ProduceResponsePayloadTest {

    public static void main(String[] args) {

        ProduceResponsePayload original = new ProduceResponsePayload(42);

        byte[] encoded = original.encode();
        System.out.println("Encoded payload size: " + encoded.length);
        ProduceResponsePayload decoded = ProduceResponsePayload.decode(encoded);
        System.out.println("Offset: " + decoded.offset());
    }
}
package com.buildyourownkafka.client;

import com.buildyourownkafka.protocol.ProduceBatchResponsePayload;

public class ProduceBatchResponsePayloadTest {

    public static void main(String[] args) {

        ProduceBatchResponsePayload original =
                new ProduceBatchResponsePayload(10, 3);

        byte[] encoded = original.encode();

        ProduceBatchResponsePayload decoded =
                ProduceBatchResponsePayload.decode(encoded);

        System.out.println(
                "Base offset: " + decoded.baseOffset()
        );

        System.out.println(
                "Record count: " + decoded.recordCount()
        );

        for (int i = 0; i < decoded.recordCount(); i++) {
            System.out.println(
                    "Record offset: " + (decoded.baseOffset() + i)
            );
        }
    }
}
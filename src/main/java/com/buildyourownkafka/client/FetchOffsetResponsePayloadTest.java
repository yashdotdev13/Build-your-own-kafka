package com.buildyourownkafka.client;

import com.buildyourownkafka.protocol.FetchOffsetResponsePayload;

public class FetchOffsetResponsePayloadTest {

    public static void main(String[] args) {

        System.out.println(
                "=== FETCH OFFSET RESPONSE TEST ==="
        );

        FetchOffsetResponsePayload payload =
                new FetchOffsetResponsePayload(7);

        byte[] bytes =
                payload.encode();

        FetchOffsetResponsePayload decoded =
                FetchOffsetResponsePayload.decode(
                        bytes
                );

        System.out.println(
                "Decoded offset: "
                        + decoded.offset()
        );
    }
}
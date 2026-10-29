package com.buildyourownkafka.protocol;

import java.nio.ByteBuffer;

public record FetchOffsetResponsePayload(long offset) {

    public FetchOffsetResponsePayload {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }
    }

    public byte[] encode() {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(offset);
        return buffer.array();
    }

    public static FetchOffsetResponsePayload decode(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }

        if (bytes.length != Long.BYTES) {
            throw new IllegalArgumentException(
                    "FETCH_OFFSET response payload must be exactly 8 bytes"
            );
        }

        long offset = ByteBuffer.wrap(bytes).getLong();

        return new FetchOffsetResponsePayload(offset);
    }
}

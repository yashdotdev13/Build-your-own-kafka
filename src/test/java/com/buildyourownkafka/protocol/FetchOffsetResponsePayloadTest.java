package com.buildyourownkafka.protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class FetchOffsetResponsePayloadTest {

    @Test
    void shouldRoundTripValidOffset() {
        FetchOffsetResponsePayload original =
                new FetchOffsetResponsePayload(42L);

        FetchOffsetResponsePayload decoded =
                FetchOffsetResponsePayload.decode(original.encode());

        assertEquals(42L, decoded.offset());
    }

    @Test
    void shouldAcceptZeroOffset() {
        FetchOffsetResponsePayload decoded =
                FetchOffsetResponsePayload.decode(
                        ByteBuffer.allocate(Long.BYTES).putLong(0L).array()
                );

        assertEquals(0L, decoded.offset());
    }

    @Test
    void shouldRejectNegativeOffset() {
        byte[] payload =
                ByteBuffer.allocate(Long.BYTES).putLong(-1L).array();

        assertThrows(
                IllegalArgumentException.class,
                () -> FetchOffsetResponsePayload.decode(payload)
        );
    }

    @Test
    void shouldRejectNullPayload() {
        assertThrows(
                IllegalArgumentException.class,
                () -> FetchOffsetResponsePayload.decode(null)
        );
    }

    @Test
    void shouldRejectTruncatedPayload() {
        byte[] payload = new byte[Long.BYTES - 1];

        assertThrows(
                IllegalArgumentException.class,
                () -> FetchOffsetResponsePayload.decode(payload)
        );
    }

    @Test
    void shouldRejectEmptyPayload() {
        assertThrows(
                IllegalArgumentException.class,
                () -> FetchOffsetResponsePayload.decode(new byte[0])
        );
    }

    @Test
    void shouldRejectTrailingBytes() {
        byte[] payload =
                ByteBuffer.allocate(Long.BYTES + 1)
                        .putLong(42L)
                        .put((byte) 1)
                        .array();

        assertThrows(
                IllegalArgumentException.class,
                () -> FetchOffsetResponsePayload.decode(payload)
        );
    }
}

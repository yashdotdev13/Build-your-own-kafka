
package com.buildyourownkafka.protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class ProduceResponsePayloadTest {

    @Test
    void shouldRoundTripValidOffset() {
        ProduceResponsePayload original =
                new ProduceResponsePayload(42L);

        ProduceResponsePayload decoded =
                ProduceResponsePayload.decode(original.encode());

        assertEquals(42L, decoded.offset());
    }

    @Test
    void shouldAcceptZeroOffset() {
        byte[] payload =
                ByteBuffer.allocate(Long.BYTES).putLong(0L).array();

        assertEquals(
                0L,
                ProduceResponsePayload.decode(payload).offset()
        );
    }

    @Test
    void shouldRejectNegativeOffset() {
        byte[] payload =
                ByteBuffer.allocate(Long.BYTES).putLong(-1L).array();

        assertThrows(
                IllegalArgumentException.class,
                () -> ProduceResponsePayload.decode(payload)
        );
    }

    @Test
    void shouldRejectNullPayload() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ProduceResponsePayload.decode(null)
        );
    }

    @Test
    void shouldRejectTruncatedPayload() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ProduceResponsePayload.decode(new byte[Long.BYTES - 1])
        );
    }

    @Test
    void shouldRejectEmptyPayload() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ProduceResponsePayload.decode(new byte[0])
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
                () -> ProduceResponsePayload.decode(payload)
        );
    }
}

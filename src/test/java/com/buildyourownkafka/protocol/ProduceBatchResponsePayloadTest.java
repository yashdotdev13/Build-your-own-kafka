
package com.buildyourownkafka.protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class ProduceBatchResponsePayloadTest {

    @Test
    void shouldRoundTripValidPayload() {
        ProduceBatchResponsePayload original =
                new ProduceBatchResponsePayload(10L, 3);

        ProduceBatchResponsePayload decoded =
                ProduceBatchResponsePayload.decode(original.encode());

        assertEquals(10L, decoded.baseOffset());
        assertEquals(3, decoded.recordCount());
    }

    @Test
    void shouldRejectNullPayload() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ProduceBatchResponsePayload.decode(null)
        );
    }

    @Test
    void shouldRejectEmptyPayload() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ProduceBatchResponsePayload.decode(new byte[0])
        );
    }

    @Test
    void shouldRejectTruncatedPayload() {
        // Valid offset, but only 3 bytes of the 4-byte record count.
        byte[] payload = ByteBuffer.allocate(11)
                .putLong(10L)
                .put((byte) 0)
                .put((byte) 0)
                .put((byte) 0)
                .array();

        assertThrows(
                IllegalArgumentException.class,
                () -> ProduceBatchResponsePayload.decode(payload)
        );
    }

    @Test
    void shouldRejectTrailingBytes() {
        byte[] payload = ByteBuffer.allocate(13)
                .putLong(10L)
                .putInt(3)
                .put((byte) 1)
                .array();

        assertThrows(
                IllegalArgumentException.class,
                () -> ProduceBatchResponsePayload.decode(payload)
        );
    }

    @Test
    void shouldRejectNegativeBaseOffset() {
        byte[] payload = ByteBuffer.allocate(12)
                .putLong(-1L)
                .putInt(1)
                .array();

        assertThrows(
                IllegalArgumentException.class,
                () -> ProduceBatchResponsePayload.decode(payload)
        );
    }

    @Test
    void shouldRejectZeroRecordCount() {
        byte[] payload = ByteBuffer.allocate(12)
                .putLong(10L)
                .putInt(0)
                .array();

        assertThrows(
                IllegalArgumentException.class,
                () -> ProduceBatchResponsePayload.decode(payload)
        );
    }

    @Test
    void shouldRejectNegativeRecordCount() {
        byte[] payload = ByteBuffer.allocate(12)
                .putLong(10L)
                .putInt(-1)
                .array();

        assertThrows(
                IllegalArgumentException.class,
                () -> ProduceBatchResponsePayload.decode(payload)
        );
    }
}

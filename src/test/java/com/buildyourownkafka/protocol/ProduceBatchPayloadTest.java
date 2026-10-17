package com.buildyourownkafka.protocol;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProduceBatchPayloadTest {

    @Test
    void shouldEncodeAndDecodeBatchPayload() {
        ProduceBatchPayload original = new ProduceBatchPayload("orders", 1,
                List.of(bytes("order-101"), bytes("order-102"), bytes("order-103")));
        ProduceBatchPayload decoded = ProduceBatchPayload.decode(original.encode());
        assertEquals("orders", decoded.topicName());
        assertEquals(1, decoded.partitionId());
        assertEquals(3, decoded.records().size());

        assertRecord(decoded.records().get(0), "order-101");
        assertRecord(decoded.records().get(1), "order-102");
        assertRecord(decoded.records().get(2), "order-103");
    }

    @Test
    void shouldSupportEmptyRecordValues() {
        ProduceBatchPayload original = new ProduceBatchPayload("orders", 0, List.of(new byte[0]));
        ProduceBatchPayload decoded = ProduceBatchPayload.decode(original.encode());
        assertEquals(1, decoded.records().size());
        assertEquals(0, decoded.records().get(0).length);
    }
    @Test
    void shouldRejectBlankTopicName() {
        assertThrows(IllegalArgumentException.class, () -> new ProduceBatchPayload(" ", 0,
                List.of(bytes("order"))));
    }
    @Test
    void shouldRejectNegativePartitionId() {
        assertThrows(IllegalArgumentException.class, () -> new ProduceBatchPayload("orders", -1,
                List.of(bytes("order"))));
    }
    @Test
    void shouldRejectEmptyBatch() {
        assertThrows(IllegalArgumentException.class, () -> new ProduceBatchPayload("orders", 0,
                List.of()));
    }
    @Test
    void shouldRejectNullRecordsList() {
        assertThrows(IllegalArgumentException.class, () -> new ProduceBatchPayload("orders", 0,
                null));
    }

    @Test
    void shouldRejectNullRecordValue() {
        assertThrows(IllegalArgumentException.class, () -> new ProduceBatchPayload("orders", 0,
                java.util.Arrays.asList((byte[]) null)));
    }
    @Test
    void shouldRejectNullEncodedPayload() {
        assertThrows(IllegalArgumentException.class, () -> ProduceBatchPayload.decode(null));
    }
    @Test
    void shouldRejectTruncatedPayload() {
        byte[] encoded = new ProduceBatchPayload("orders", 0, List.of(bytes("order-101")))
                .encode();
        byte[] truncated = java.util.Arrays.copyOf(encoded, encoded.length - 1);
        assertThrows(IllegalArgumentException.class, () -> ProduceBatchPayload.decode(truncated));
    }

    @Test
    void shouldRejectTrailingBytes() {
        byte[] encoded = new ProduceBatchPayload("orders", 0, List.of(bytes("order-101")))
                .encode();
        byte[] withTrailingByte = java.util.Arrays.copyOf(encoded, encoded.length + 1);
        assertThrows(IllegalArgumentException.class, () -> ProduceBatchPayload.decode(withTrailingByte));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void assertRecord(byte[] actual, String expected) {
        assertArrayEquals(bytes(expected), actual);
    }
}

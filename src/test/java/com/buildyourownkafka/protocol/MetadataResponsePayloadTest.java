package com.buildyourownkafka.protocol;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MetadataResponsePayloadTest {

    @Test
    void shouldRoundTripValidPayload() {
        MetadataResponsePayload original =
                new MetadataResponsePayload("orders", List.of(0, 1, 2));

        MetadataResponsePayload decoded =
                MetadataResponsePayload.decode(original.encode());

        assertEquals("orders", decoded.topicName());
        assertEquals(List.of(0, 1, 2), decoded.partitionIds());
        assertEquals(3, decoded.partitionCount());
    }

    @Test
    void shouldRejectNullPayload() {
        assertThrows(
                IllegalArgumentException.class,
                () -> MetadataResponsePayload.decode(null)
        );
    }

    @Test
    void shouldRejectNegativePartitionCount() throws IOException {
        byte[] payload = payloadWith(out -> {
            writeTopic(out, "orders");
            out.writeInt(-1);
        });

        assertThrows(
                IllegalArgumentException.class,
                () -> MetadataResponsePayload.decode(payload)
        );
    }

    @Test
    void shouldRejectExcessivePartitionCount() throws IOException {
        byte[] payload = payloadWith(out -> {
            writeTopic(out, "orders");
            out.writeInt(Integer.MAX_VALUE);
        });

        assertThrows(
                IllegalArgumentException.class,
                () -> MetadataResponsePayload.decode(payload)
        );
    }

    @Test
    void shouldRejectTruncatedPartitionList() throws IOException {
        byte[] payload = payloadWith(out -> {
            writeTopic(out, "orders");
            out.writeInt(2);
            out.writeInt(0);
        });

        assertThrows(
                IllegalArgumentException.class,
                () -> MetadataResponsePayload.decode(payload)
        );
    }

    @Test
    void shouldRejectNegativePartitionId() throws IOException {
        byte[] payload = payloadWith(out -> {
            writeTopic(out, "orders");
            out.writeInt(1);
            out.writeInt(-1);
        });

        assertThrows(
                IllegalArgumentException.class,
                () -> MetadataResponsePayload.decode(payload)
        );
    }

    @Test
    void shouldRejectTrailingBytes() throws IOException {
        byte[] payload = payloadWith(out -> {
            writeTopic(out, "orders");
            out.writeInt(1);
            out.writeInt(0);
            out.writeByte(99);
        });

        assertThrows(
                IllegalArgumentException.class,
                () -> MetadataResponsePayload.decode(payload)
        );
    }

    @Test
    void shouldRejectZeroPartitionCount() throws IOException {
        byte[] payload = payloadWith(out -> {
            writeTopic(out, "orders");
            out.writeInt(0);
        });

        assertThrows(
                IllegalArgumentException.class,
                () -> MetadataResponsePayload.decode(payload)
        );
    }

    private static void writeTopic(DataOutputStream out, String topic)
            throws IOException {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        out.writeInt(topicBytes.length);
        out.write(topicBytes);
    }

    @FunctionalInterface
    private interface PayloadWriter {
        void write(DataOutputStream out) throws IOException;
    }

    private static byte[] payloadWith(PayloadWriter writer)
            throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        try (DataOutputStream out = new DataOutputStream(buffer)) {
            writer.write(out);
        }

        return buffer.toByteArray();
    }
}

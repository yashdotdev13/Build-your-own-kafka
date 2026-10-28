package com.buildyourownkafka.protocol;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class MetadataPayloadTest {

    @Test
    void shouldDecodeValidPayload() {
        MetadataPayload original = new MetadataPayload("orders");

        MetadataPayload decoded =
                MetadataPayload.decode(original.encode());

        assertEquals(original, decoded);
    }

    @Test
    void shouldRejectNullPayload() {
        assertThrows(
                IllegalArgumentException.class,
                () -> MetadataPayload.decode(null)
        );
    }

    @Test
    void shouldRejectZeroTopicLength() throws IOException {
        assertThrows(
                IllegalArgumentException.class,
                () -> MetadataPayload.decode(payloadWithTopicLength(0))
        );
    }

    @Test
    void shouldRejectNegativeTopicLength() throws IOException {
        assertThrows(
                IllegalArgumentException.class,
                () -> MetadataPayload.decode(payloadWithTopicLength(-1))
        );
    }

    @Test
    void shouldRejectTopicLengthAbove255Bytes() throws IOException {
        assertThrows(
                IllegalArgumentException.class,
                () -> MetadataPayload.decode(payloadWithTopicLength(256))
        );
    }

    @Test
    void shouldRejectTruncatedTopic() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(10);
            output.write("abc".getBytes(StandardCharsets.UTF_8));
        }

        assertThrows(
                IllegalArgumentException.class,
                () -> MetadataPayload.decode(bytes.toByteArray())
        );
    }

    @Test
    void shouldRejectTrailingBytes() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.write(new MetadataPayload("orders").encode());
        bytes.write(99);

        assertThrows(
                IllegalArgumentException.class,
                () -> MetadataPayload.decode(bytes.toByteArray())
        );
    }

    private byte[] payloadWithTopicLength(int length) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(length);
        }

        return bytes.toByteArray();
    }
}

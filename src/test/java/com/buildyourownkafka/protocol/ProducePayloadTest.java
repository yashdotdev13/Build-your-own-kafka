
package com.buildyourownkafka.protocol;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ProducePayloadTest {

    @Test
    void shouldDecodeValidPayload() {
        ProducePayload original =
                new ProducePayload("orders", 0, new byte[]{1, 2, 3});

        ProducePayload decoded = ProducePayload.decode(original.encode());

        assertEquals(original.topicName(), decoded.topicName());
        assertEquals(original.partitionId(), decoded.partitionId());
        assertArrayEquals(original.value(), decoded.value());
    }

    @Test
    void shouldDecodeEmptyRecordValue() {
        ProducePayload original =
                new ProducePayload("orders", 0, new byte[0]);

        ProducePayload decoded = ProducePayload.decode(original.encode());

        assertArrayEquals(new byte[0], decoded.value());
    }

    @Test
    void shouldRejectTrailingBytes() {
        byte[] valid = new ProducePayload(
                "orders", 0, new byte[]{1, 2, 3}
        ).encode();

        byte[] malformed = java.util.Arrays.copyOf(valid, valid.length + 1);
        malformed[malformed.length - 1] = 99;

        assertThrows(
                IllegalArgumentException.class,
                () -> ProducePayload.decode(malformed)
        );
    }

    @Test
    void shouldRejectNegativeTopicLength() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(-1);
        }

        assertThrows(
                IllegalArgumentException.class,
                () -> ProducePayload.decode(bytes.toByteArray())
        );
    }

    @Test
    void shouldRejectNegativePartition() {
        byte[] malformed = payloadWithTopicAndPartition("orders", -1);

        assertThrows(
                IllegalArgumentException.class,
                () -> ProducePayload.decode(malformed)
        );
    }

    @Test
    void shouldRejectNegativeValueLength() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (DataOutputStream output = new DataOutputStream(bytes)) {
            byte[] topic = "orders".getBytes(StandardCharsets.UTF_8);

            output.writeInt(topic.length);
            output.write(topic);
            output.writeInt(0);
            output.writeInt(-1);
        }

        assertThrows(
                IllegalArgumentException.class,
                () -> ProducePayload.decode(bytes.toByteArray())
        );
    }

    @Test
    void shouldRejectTruncatedRecordValue() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (DataOutputStream output = new DataOutputStream(bytes)) {
            byte[] topic = "orders".getBytes(StandardCharsets.UTF_8);

            output.writeInt(topic.length);
            output.write(topic);
            output.writeInt(0);
            output.writeInt(100);
            output.write(new byte[]{1, 2, 3});
        }

        assertThrows(
                IllegalArgumentException.class,
                () -> ProducePayload.decode(bytes.toByteArray())
        );
    }


    @Test
    void shouldRejectExcessiveDeclaredValueLength() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (DataOutputStream output = new DataOutputStream(bytes)) {
            byte[] topic = "orders".getBytes(StandardCharsets.UTF_8);

            output.writeInt(topic.length);
            output.write(topic);
            output.writeInt(0);              // Partition
            output.writeInt(Integer.MAX_VALUE); // Declared value length
            output.write(new byte[]{1, 2, 3});  // Only 3 bytes provided
        }

        assertThrows(
                IllegalArgumentException.class,
                () -> ProducePayload.decode(bytes.toByteArray())
        );
    }


    private byte[] payloadWithTopicAndPartition(
            String topicName,
            int partition
    ) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();

            try (DataOutputStream output = new DataOutputStream(bytes)) {
                byte[] topic = topicName.getBytes(StandardCharsets.UTF_8);

                output.writeInt(topic.length);
                output.write(topic);
                output.writeInt(partition);
            }

            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}

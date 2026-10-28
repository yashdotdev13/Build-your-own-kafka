
package com.buildyourownkafka.protocol;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class CommitOffsetPayloadTest {

    @Test
    void shouldDecodeValidPayload() {
        CommitOffsetPayload original =
                new CommitOffsetPayload("group-1", "orders", 0, 42L);

        CommitOffsetPayload decoded =
                CommitOffsetPayload.decode(original.encode());

        assertEquals(original, decoded);
    }

    @Test
    void shouldRejectTrailingBytes() throws IOException {
        CommitOffsetPayload original =
                new CommitOffsetPayload("group-1", "orders", 0, 42L);

        byte[] validPayload = original.encode();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(validPayload);
        output.write(99); // Unexpected trailing byte

        assertThrows(
                RuntimeException.class,
                () -> CommitOffsetPayload.decode(output.toByteArray())
        );
    }


    @Test
    void shouldRejectEmptyGroupId() throws IOException {
        byte[] payload = commitOffsetBytes(
                new byte[0],
                "orders".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                0,
                42L
        );

        assertThrows(
                RuntimeException.class,
                () -> CommitOffsetPayload.decode(payload)
        );
    }

    @Test
    void shouldRejectEmptyTopic() throws IOException {
        byte[] payload = commitOffsetBytes(
                "group-1".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                new byte[0],
                0,
                42L
        );

        assertThrows(
                RuntimeException.class,
                () -> CommitOffsetPayload.decode(payload)
        );
    }

    private byte[] commitOffsetBytes(
            byte[] groupId,
            byte[] topic,
            int partition,
            long offset
    ) throws IOException {
        ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();

        try (DataOutputStream output = new DataOutputStream(outputBytes)) {
            output.writeInt(groupId.length);
            output.write(groupId);
            output.writeInt(topic.length);
            output.write(topic);
            output.writeInt(partition);
            output.writeLong(offset);
        }

        return outputBytes.toByteArray();
    }


    @Test
    void shouldRejectNegativeGroupIdLength() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(-1);
        }

        assertThrows(
                RuntimeException.class,
                () -> CommitOffsetPayload.decode(bytes.toByteArray())
        );
    }

    @Test
    void shouldRejectNegativeTopicLength() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (DataOutputStream output = new DataOutputStream(bytes)) {
            byte[] groupId = "group-1".getBytes(
                    java.nio.charset.StandardCharsets.UTF_8
            );

            output.writeInt(groupId.length);
            output.write(groupId);
            output.writeInt(-1);
        }

        assertThrows(
                RuntimeException.class,
                () -> CommitOffsetPayload.decode(bytes.toByteArray())
        );
    }


    @Test
    void shouldRejectGroupIdLengthExceedingAvailableBytes() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(100); // Claims 100 group-ID bytes
            output.write("short".getBytes(
                    java.nio.charset.StandardCharsets.UTF_8
            )); // Only 5 bytes provided
        }

        assertThrows(
                RuntimeException.class,
                () -> CommitOffsetPayload.decode(bytes.toByteArray())
        );
    }


    @Test
    void shouldRejectGroupIdLengthLargerThanPayload() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(Integer.MAX_VALUE);
        }

        assertThrows(
                RuntimeException.class,
                () -> CommitOffsetPayload.decode(bytes.toByteArray())
        );
    }




}

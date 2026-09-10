package com.buildyourownkafka.protocol;

import java.io.*;
import java.nio.charset.StandardCharsets;

public record ProducePayload(String topicName, int partitionId, byte[] value) {

    public ProducePayload {
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be null or blank");
        }
        if (partitionId < 0) {
            throw new IllegalArgumentException("Partition id cannot be negative");
        }
        if (value == null) {
            throw new IllegalArgumentException("Record value cannot be null");
        }
    }

    public byte[] encode() {

        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(output);

            byte[] topicBytes = topicName.getBytes(StandardCharsets.UTF_8);

            data.writeInt(topicBytes.length);
            data.write(topicBytes);

            data.writeInt(partitionId);

            data.writeInt(value.length);
            data.write(value);

            data.flush();

            return output.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode PRODUCE payload", e);
        }
    }

    public static ProducePayload decode(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }
        try {
            ByteArrayInputStream input = new ByteArrayInputStream(bytes);
            DataInputStream data = new DataInputStream(input);

            int topicLength = data.readInt();
            if (topicLength <= 0 || topicLength > 255) {
                throw new IllegalArgumentException("Invalid topic name length: " + topicLength);
            }

            byte[] topicBytes = new byte[topicLength];
            data.readFully(topicBytes);
            String topicName = new String(topicBytes, StandardCharsets.UTF_8);

            int partitionId = data.readInt();
            if (partitionId < 0) {
                throw new IllegalArgumentException("Partition id cannot be negative");
            }

            int valueLength = data.readInt();
            if (valueLength < 0) {
                throw new IllegalArgumentException("Record value length cannot be negative");
            }

            byte[] value = new byte[valueLength];
            data.readFully(value);

            if (data.available() != 0) {
                throw new IllegalArgumentException("Unexpected trailing bytes in PRODUCE payload");
            }
            return new ProducePayload(topicName, partitionId, value);
        } catch (EOFException e) {
            throw new IllegalArgumentException("Invalid PRODUCE payload", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to decode PRODUCE payload", e);
        }
    }
}
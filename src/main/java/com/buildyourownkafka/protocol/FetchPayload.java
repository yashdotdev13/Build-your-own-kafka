package com.buildyourownkafka.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public record FetchPayload(String topicName, int partitionId, long offset) {

    public FetchPayload {

        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be null or blank");
        }
        if (partitionId < 0) {
            throw new IllegalArgumentException("Partition id cannot be negative");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
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
            data.writeLong(offset);
            data.flush();
            return output.toByteArray();

        } catch (IOException e) {

            throw new IllegalStateException("Failed to encode FETCH payload", e);
        }
    }

    public static FetchPayload decode(byte[] bytes) {
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
            long offset = data.readLong();
            if (offset < 0) {
                throw new IllegalArgumentException("Offset cannot be negative");
            }
            if (data.available() != 0) {
                throw new IllegalArgumentException("Unexpected trailing bytes in FETCH payload");
            }
            return new FetchPayload(topicName, partitionId, offset);

        } catch (EOFException e) {
            throw new IllegalArgumentException("Invalid FETCH payload", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to decode FETCH payload", e);
        }
    }
}
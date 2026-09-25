package com.buildyourownkafka.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public record MetadataResponsePayload(String topicName, List<Integer> partitionIds) {

    public MetadataResponsePayload {
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be null or blank");
        }

        if (partitionIds == null || partitionIds.isEmpty()) {
            throw new IllegalArgumentException("Partition list cannot be null or empty");
        }

        partitionIds = List.copyOf(partitionIds);

        for (Integer partitionId : partitionIds) {
            if (partitionId == null || partitionId < 0) {
                throw new IllegalArgumentException("Partition id cannot be negative");
            }
        }
    }

    public int partitionCount() {
        return partitionIds.size();
    }

    public byte[] encode() {

        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(output);
            byte[] topicBytes = topicName.getBytes(StandardCharsets.UTF_8);
            data.writeInt(topicBytes.length);
            data.write(topicBytes);
            data.writeInt(partitionIds.size());
            for (int partitionId : partitionIds) {
                data.writeInt(partitionId);
            }
            data.flush();
            return output.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode METADATA response", e);
        }
    }
    public static MetadataResponsePayload decode(byte[] bytes) {
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
            int partitionCount = data.readInt();
            if (partitionCount <= 0) {
                throw new IllegalArgumentException("Partition count must be greater than zero");
            }
            List<Integer> partitionIds = new java.util.ArrayList<>();
            for (int i = 0; i < partitionCount; i++) {
                int partitionId = data.readInt();

                if (partitionId < 0) {
                    throw new IllegalArgumentException("Partition id cannot be negative");
                }
                partitionIds.add(partitionId);
            }
            if (data.available() != 0) {
                throw new IllegalArgumentException("Unexpected trailing bytes in METADATA response");
            }
            return new MetadataResponsePayload(topicName, partitionIds);

        } catch (EOFException e) {
            throw new IllegalArgumentException("Invalid METADATA response payload", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to decode METADATA response payload", e);
        }
    }
}
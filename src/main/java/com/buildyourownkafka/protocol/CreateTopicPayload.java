package com.buildyourownkafka.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public record CreateTopicPayload(String topicName, int partitionCount) {

    public CreateTopicPayload {

        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be empty");
        }
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("Partition count must be greater than zero");
        }
    }

    public byte[] encode() {

        try {
            byte[] topicNameBytes = topicName.getBytes(StandardCharsets.UTF_8);
            ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(byteOutput);
            output.writeInt(topicNameBytes.length);
            output.write(topicNameBytes);
            output.writeInt(partitionCount);
            output.flush();
            return byteOutput.toByteArray();
        } catch (IOException e) {

            throw new IllegalStateException("Failed to encode CREATE_TOPIC payload", e);
        }
    }

    public static CreateTopicPayload decode(byte[] payload) {

        try {

            ByteArrayInputStream byteInput = new ByteArrayInputStream(payload);
            DataInputStream input = new DataInputStream(byteInput);
            int topicNameLength = input.readInt();
            if (topicNameLength <= 0) {
                throw new IllegalArgumentException("Invalid topic name length: " + topicNameLength);
            }
            if (topicNameLength > 255) {
                throw new IllegalArgumentException("Topic name is too long");
            }
            byte[] topicNameBytes = new byte[topicNameLength];
            input.readFully(topicNameBytes);
            String topicName = new String(topicNameBytes, StandardCharsets.UTF_8);
            int partitionCount = input.readInt();
            if (partitionCount <= 0) {
                throw new IllegalArgumentException("Invalid partition count: " + partitionCount);
            }
            if (input.available() != 0) {
                throw new IllegalArgumentException("Unexpected bytes in CREATE_TOPIC payload");
            }
            return new CreateTopicPayload(topicName, partitionCount);

        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid CREATE_TOPIC payload", e);
        }
    }
}
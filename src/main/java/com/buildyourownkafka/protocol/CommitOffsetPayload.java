package com.buildyourownkafka.protocol;

import java.io.*;
import java.nio.charset.StandardCharsets;

public record CommitOffsetPayload(
        String groupId,
        String topic,
        int partition,
        long offset
) {

    public CommitOffsetPayload {

        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException(
                    "Group ID cannot be blank"
            );
        }

        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException(
                    "Topic cannot be blank"
            );
        }

        if (partition < 0) {
            throw new IllegalArgumentException(
                    "Partition cannot be negative"
            );
        }

        if (offset < 0) {
            throw new IllegalArgumentException(
                    "Offset cannot be negative"
            );
        }
    }

    public byte[] encode() {

        try {

            ByteArrayOutputStream byteOutput =
                    new ByteArrayOutputStream();

            DataOutputStream output =
                    new DataOutputStream(byteOutput);

            byte[] groupIdBytes =
                    groupId.getBytes(StandardCharsets.UTF_8);

            byte[] topicBytes =
                    topic.getBytes(StandardCharsets.UTF_8);

            output.writeInt(groupIdBytes.length);
            output.write(groupIdBytes);

            output.writeInt(topicBytes.length);
            output.write(topicBytes);

            output.writeInt(partition);

            output.writeLong(offset);

            output.flush();

            return byteOutput.toByteArray();

        } catch (IOException e) {

            throw new RuntimeException(
                    "Failed to encode commit offset payload",
                    e
            );
        }
    }

    public static CommitOffsetPayload decode(
            byte[] bytes) {

        try {

            DataInputStream input =
                    new DataInputStream(
                            new ByteArrayInputStream(bytes)
                    );

            int groupIdLength =
                    input.readInt();

            if (groupIdLength < 0) {
                throw new IOException(
                        "Invalid group ID length"
                );
            }

            byte[] groupIdBytes =
                    input.readNBytes(groupIdLength);

            if (groupIdBytes.length != groupIdLength) {
                throw new EOFException(
                        "Incomplete group ID"
                );
            }

            String groupId =
                    new String(
                            groupIdBytes,
                            StandardCharsets.UTF_8
                    );

            int topicLength =
                    input.readInt();

            if (topicLength < 0) {
                throw new IOException(
                        "Invalid topic length"
                );
            }

            byte[] topicBytes =
                    input.readNBytes(topicLength);

            if (topicBytes.length != topicLength) {
                throw new EOFException(
                        "Incomplete topic"
                );
            }

            String topic =
                    new String(
                            topicBytes,
                            StandardCharsets.UTF_8
                    );

            int partition =
                    input.readInt();

            long offset =
                    input.readLong();

            return new CommitOffsetPayload(
                    groupId,
                    topic,
                    partition,
                    offset
            );

        } catch (IOException e) {

            throw new RuntimeException(
                    "Failed to decode commit offset payload",
                    e
            );
        }
    }
}
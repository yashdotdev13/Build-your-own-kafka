package com.buildyourownkafka.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public record MetadataPayload(String topicName) {

    public MetadataPayload {
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be null or blank");
        }
    }

    public byte[] encode() {
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(output);

            byte[] topicBytes = topicName.getBytes(StandardCharsets.UTF_8);

            data.writeInt(topicBytes.length);
            data.write(topicBytes);

            data.flush();

            return output.toByteArray();

        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to encode METADATA payload",
                    e
            );
        }
    }

    public static MetadataPayload decode(byte[] bytes) {

        if (bytes == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }

        try {
            ByteArrayInputStream input =
                    new ByteArrayInputStream(bytes);

            DataInputStream data =
                    new DataInputStream(input);

            int topicLength = data.readInt();

            if (topicLength <= 0 || topicLength > 255) {
                throw new IllegalArgumentException(
                        "Invalid topic name length: " + topicLength
                );
            }

            byte[] topicBytes = new byte[topicLength];

            data.readFully(topicBytes);

            String topicName =
                    new String(topicBytes, StandardCharsets.UTF_8);

            if (data.available() != 0) {
                throw new IllegalArgumentException(
                        "Unexpected trailing bytes in METADATA payload"
                );
            }

            return new MetadataPayload(topicName);

        } catch (EOFException e) {
            throw new IllegalArgumentException(
                    "Invalid METADATA payload",
                    e
            );
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Failed to decode METADATA payload",
                    e
            );
        }
    }
}


package com.buildyourownkafka.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public record ProduceBatchPayload(
        String topicName,
        int partitionId,
        List<byte[]> records
) {

    public ProduceBatchPayload {
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException(
                    "Topic name cannot be null or blank"
            );
        }

        if (partitionId < 0) {
            throw new IllegalArgumentException(
                    "Partition id cannot be negative"
            );
        }

        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException(
                    "Batch must contain at least one record"
            );
        }

        if (records.stream().anyMatch(record -> record == null)) {
            throw new IllegalArgumentException(
                    "Record value cannot be null"
            );
        }

        records = List.copyOf(records);
    }

    public byte[] encode() {
        try {
            ByteArrayOutputStream output =
                    new ByteArrayOutputStream();

            DataOutputStream data =
                    new DataOutputStream(output);

            byte[] topicBytes =
                    topicName.getBytes(StandardCharsets.UTF_8);

            data.writeInt(topicBytes.length);
            data.write(topicBytes);

            data.writeInt(partitionId);

            data.writeInt(records.size());

            for (byte[] record : records) {
                data.writeInt(record.length);
                data.write(record);
            }

            data.flush();

            return output.toByteArray();

        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to encode PRODUCE batch payload",
                    e
            );
        }
    }

    public static ProduceBatchPayload decode(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException(
                    "Payload cannot be null"
            );
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

            int partitionId = data.readInt();

            if (partitionId < 0) {
                throw new IllegalArgumentException(
                        "Partition id cannot be negative"
                );
            }

            int recordCount = data.readInt();

            if (recordCount <= 0) {
                throw new IllegalArgumentException(
                        "Batch must contain at least one record"
                );
            }

            List<byte[]> records =
                    new ArrayList<>(recordCount);

            for (int i = 0; i < recordCount; i++) {
                int recordLength = data.readInt();

                if (recordLength < 0) {
                    throw new IllegalArgumentException(
                            "Record value length cannot be negative"
                    );
                }

                byte[] record = new byte[recordLength];
                data.readFully(record);

                records.add(record);
            }

            if (data.available() != 0) {
                throw new IllegalArgumentException(
                        "Unexpected trailing bytes in PRODUCE batch payload"
                );
            }

            return new ProduceBatchPayload(
                    topicName,
                    partitionId,
                    records
            );

        } catch (EOFException e) {
            throw new IllegalArgumentException(
                    "Invalid PRODUCE batch payload",
                    e
            );
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Failed to decode PRODUCE batch payload",
                    e
            );
        }
    }
}
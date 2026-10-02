package com.buildyourownkafka.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

public record ProduceBatchResponsePayload(
        long baseOffset,
        int recordCount
) {

    public ProduceBatchResponsePayload {
        if (baseOffset < 0) {
            throw new IllegalArgumentException(
                    "Base offset cannot be negative"
            );
        }

        if (recordCount <= 0) {
            throw new IllegalArgumentException(
                    "Record count must be greater than zero"
            );
        }
    }

    public byte[] encode() {
        try {
            ByteArrayOutputStream output =
                    new ByteArrayOutputStream();

            DataOutputStream data =
                    new DataOutputStream(output);

            data.writeLong(baseOffset);
            data.writeInt(recordCount);
            data.flush();

            return output.toByteArray();

        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to encode PRODUCE batch response",
                    e
            );
        }
    }

    public static ProduceBatchResponsePayload decode(byte[] bytes) {
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

            long baseOffset = data.readLong();
            int recordCount = data.readInt();

            if (data.available() != 0) {
                throw new IllegalArgumentException(
                        "Unexpected trailing bytes in PRODUCE batch response"
                );
            }

            return new ProduceBatchResponsePayload(
                    baseOffset,
                    recordCount
            );

        } catch (EOFException e) {
            throw new IllegalArgumentException(
                    "Invalid PRODUCE batch response",
                    e
            );
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Failed to decode PRODUCE batch response",
                    e
            );
        }
    }
}
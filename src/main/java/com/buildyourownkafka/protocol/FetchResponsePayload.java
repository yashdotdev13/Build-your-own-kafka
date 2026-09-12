package com.buildyourownkafka.protocol;

import com.buildyourownkafka.broker.Record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public record FetchResponsePayload(List<Record> records) {

    public FetchResponsePayload {
        if (records == null) {
            throw new IllegalArgumentException("Records cannot be null");
        }
        records = List.copyOf(records);
    }
    public byte[] encode() {
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(output);

            // Number of records
            data.writeInt(records.size());
            for (Record record : records) {

                // Offset
                data.writeLong(record.offset());

                // Value
                byte[] value = record.value();
                data.writeInt(value.length);
                data.write(value);
            }

            data.flush();
            return output.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode FETCH response payload", e);
        }
    }
    public static FetchResponsePayload decode(byte[] bytes) {

        if (bytes == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }
        try {
            ByteArrayInputStream input = new ByteArrayInputStream(bytes);

            DataInputStream data = new DataInputStream(input);
            int recordCount = data.readInt();

            if (recordCount < 0) {
                throw new IllegalArgumentException("Record count cannot be negative");
            }

            List<Record> records = new ArrayList<>(recordCount);
            for (int i = 0; i < recordCount; i++) {
                long offset = data.readLong();
                if (offset < 0) {
                    throw new IllegalArgumentException("Offset cannot be negative");
                }

                int valueLength = data.readInt();
                if (valueLength < 0) {
                    throw new IllegalArgumentException("Record value length cannot be negative");
                }

                byte[] value = new byte[valueLength];
                data.readFully(value);
                records.add(new Record(offset, value));
            }
            if (data.available() != 0) {
                throw new IllegalArgumentException("Unexpected trailing bytes in FETCH response payload");
            }
            return new FetchResponsePayload(records);

        } catch (EOFException e) {
            throw new IllegalArgumentException("Invalid FETCH response payload", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to decode FETCH response payload", e);
        }
    }
}
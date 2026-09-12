package com.buildyourownkafka.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

public record ProduceResponsePayload(long offset) {

    public ProduceResponsePayload {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }
    }
    public byte[] encode() {
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(output);
            data.writeLong(offset);
            data.flush();
            return output.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode PRODUCE response payload", e);
        }
    }
    public static ProduceResponsePayload decode(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }
        try {
            ByteArrayInputStream input = new ByteArrayInputStream(bytes);
            DataInputStream data = new DataInputStream(input);
            long offset = data.readLong();
            if (data.available() != 0) {
                throw new IllegalArgumentException("Unexpected trailing bytes in PRODUCE response payload");
            }
            return new ProduceResponsePayload(offset);
        } catch (EOFException e) {
            throw new IllegalArgumentException("Invalid PRODUCE response payload", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to decode PRODUCE response payload", e);
        }
    }
}
package com.buildyourownkafka.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public record FetchOffsetResponsePayload(long offset) {
    public FetchOffsetResponsePayload {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }
    }

    public byte[] encode() {
        try {
            ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(byteOutput);
            output.writeLong(offset);
            output.flush();
            return byteOutput.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode fetch offset response", e);
        }
    }
    public static FetchOffsetResponsePayload decode(byte[] bytes) {
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
            long offset = input.readLong();
            return new FetchOffsetResponsePayload(offset);
        } catch (IOException e) {
            throw new RuntimeException("Failed to decode fetch offset response", e);
        }
    }
}
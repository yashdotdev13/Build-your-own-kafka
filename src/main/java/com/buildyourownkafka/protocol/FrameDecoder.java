package com.buildyourownkafka.protocol;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

public class FrameDecoder {

    private static final int MAX_FRAME_SIZE = 1024 * 1024;

    private final DataInputStream input;

    public FrameDecoder(DataInputStream input) {
        this.input = input;
    }

    public Frame decode() throws IOException {

        int length;

        try {
            length = input.readInt();
        } catch (EOFException e) {
            return null;
        }

        if (length < 0) {
            throw new IOException(
                    "Invalid frame length: " + length
            );
        }

        if (length > MAX_FRAME_SIZE) {
            throw new IOException(
                    "Frame too large: " + length
            );
        }

        byte[] payload = new byte[length];

        input.readFully(payload);

        return new Frame(payload);
    }
}
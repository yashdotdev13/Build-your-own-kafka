package com.buildyourownkafka.protocol;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class FrameDecoder {

    private static final int MAX_FRAME_SIZE = 1024 * 1024;

    public Frame decode(InputStream inputStream)
            throws IOException {

        DataInputStream dataInputStream =
                new DataInputStream(inputStream);

        int length;

        try {
            length = dataInputStream.readInt();
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

        dataInputStream.readFully(payload);

        return new Frame(payload);
    }
}
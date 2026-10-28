package com.buildyourownkafka.protocol;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

public class FrameDecoder {

    /**
     * Maximum accepted frame payload size: 1 MiB.
     *
     * The four-byte frame length prefix is not included in this limit.
     */
    public static final int MAX_FRAME_SIZE = 1024 * 1024;
    private final DataInputStream input;
    public FrameDecoder(DataInputStream input) {
        this.input = input;
    }



    public Frame decode() throws IOException {
        // EOF is clean only if no byte of a new frame was received.
        int firstByte = input.read();

        if (firstByte == -1) {
            return null;
        }

        // The first byte was received, so the other three bytes
        // of the length prefix are mandatory.
        int length;

        try {
            length = (firstByte << 24)
                    | (input.readUnsignedByte() << 16)
                    | (input.readUnsignedByte() << 8)
                    | input.readUnsignedByte();
        } catch (EOFException e) {
            throw new EOFException("Truncated frame length prefix");
        }

        if (length < 0) {
            throw new IOException("Invalid frame length: " + length);
        }

        if (length > MAX_FRAME_SIZE) {
            throw new IOException("Frame too large: " + length);
        }

        byte[] payload = new byte[length];

        try {
            input.readFully(payload);
        } catch (EOFException e) {
            throw new EOFException(
                    "Truncated frame payload: expected "
                            + length + " bytes"
            );
        }

        return new Frame(payload);
    }

}
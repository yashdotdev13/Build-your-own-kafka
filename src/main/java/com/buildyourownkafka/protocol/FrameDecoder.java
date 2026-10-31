package com.buildyourownkafka.protocol;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;

public class FrameDecoder {

    /**
     * Maximum accepted frame payload size: 1 MiB.
     * The four-byte length prefix is excluded.
     */
    public static final int MAX_FRAME_SIZE = 1024 * 1024;

    private final DataInputStream input;
    private final Socket socket;
    private final int requestTimeoutMs;
    private final int socketReadTimeoutMs;

    public FrameDecoder(DataInputStream input) {
        this(input, null, 0, 0);
    }

    public FrameDecoder(
            DataInputStream input,
            Socket socket,
            int requestTimeoutMs,
            int socketReadTimeoutMs
    ) {
        this.input = input;
        this.socket = socket;
        this.requestTimeoutMs = requestTimeoutMs;
        this.socketReadTimeoutMs = socketReadTimeoutMs;
    }

    public Frame decode() throws IOException {

        // Wait indefinitely for the first byte of a new frame.

        int firstByte = input.read();

        if (firstByte == -1) {
            return null;
        }

        // Start the absolute request deadline when the first byte arrives.
        long deadlineNanos = 0;

        if (socket != null) {
            deadlineNanos = System.nanoTime()
                    + (long) requestTimeoutMs * 1_000_000L;
        }

        int length;

        try {
            int secondByte = readUnsignedByte(deadlineNanos);
            int thirdByte = readUnsignedByte(deadlineNanos);
            int fourthByte = readUnsignedByte(deadlineNanos);

            length = (firstByte << 24)
                    | (secondByte << 16)
                    | (thirdByte << 8)
                    | fourthByte;

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
        int offset = 0;

        try {
            while (offset < length) {

                updateReadTimeout(deadlineNanos);

                int bytesRead = input.read(
                        payload,
                        offset,
                        length - offset
                );

                if (bytesRead == -1) {
                    throw new EOFException(
                            "Truncated frame payload: expected "
                                    + length + " bytes"
                    );
                }

                offset += bytesRead;
            }

        } finally {
            restoreSocketReadTimeout();
        }

        return new Frame(payload);
    }

    private int readUnsignedByte(long deadlineNanos)
            throws IOException {

        updateReadTimeout(deadlineNanos);

        return input.readUnsignedByte();
    }

    private void updateReadTimeout(long deadlineNanos)
            throws IOException {

        if (socket == null) {
            return;
        }

        long remainingNanos =
                deadlineNanos - System.nanoTime();

        if (remainingNanos <= 0) {
            throw new SocketTimeoutException(
                    "Request deadline exceeded"
            );
        }

        int remainingMs = (int) Math.max(
                1,
                (remainingNanos + 999_999L) / 1_000_000L
        );

        int effectiveTimeout = Math.min(
                socketReadTimeoutMs,
                remainingMs
        );

        socket.setSoTimeout(effectiveTimeout);
    }

    private void restoreSocketReadTimeout()
            throws IOException {

        if (socket != null) {
            socket.setSoTimeout(0);
        }
    }
}

package com.buildyourownkafka.protocol;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class RequestDecoderTest {

    @Test
    void shouldDecodeValidRequest() throws IOException {
        byte[] frame = requestFrame(Request.PING, (short) 1, 42, new byte[0]);

        Request decoded = decoder(frame).decode();

        assertNotNull(decoded);
        assertEquals(Request.PING, decoded.type());
        assertEquals(1, decoded.version());
        assertEquals(42, decoded.correlationId());
        assertArrayEquals(new byte[0], decoded.payload());
    }

    @Test
    void shouldRejectRequestWithTrailingBytes() throws IOException {
        byte[] frame = rawRequestFrame(
                Request.PING, (short) 1, 42, 0, new byte[]{99});

        assertThrows(IOException.class, () -> decoder(frame).decode());
    }

    @Test
    void shouldRejectRequestWhenDeclaredPayloadExceedsAvailableBytes()
            throws IOException {
        byte[] frame = rawRequestFrame(
                Request.PING, (short) 1, 42, 2, new byte[]{10});

        assertThrows(IOException.class, () -> decoder(frame).decode());
    }

    @Test
    void shouldRejectIncompleteRequestHeader() throws IOException {
        byte[] frame = rawFrame(new byte[]{0, 0, 0, 1, 0, 1});

        assertThrows(IOException.class, () -> decoder(frame).decode());
    }

    private static RequestDecoder decoder(byte[] bytes) {
        return new RequestDecoder(
                new DataInputStream(new ByteArrayInputStream(bytes)));
    }

    private static byte[] requestFrame(
            int type, short version, int correlationId, byte[] payload)
            throws IOException {
        return rawRequestFrame(
                type, version, correlationId, payload.length, payload);
    }

    private static byte[] rawRequestFrame(
            int type, short version, int correlationId,
            int declaredLength, byte[] payload) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(type);
            out.writeShort(version);
            out.writeInt(correlationId);
            out.writeInt(declaredLength);
            out.write(payload);
        }
        return rawFrame(bytes.toByteArray());
    }

    private static byte[] rawFrame(byte[] payload) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(payload.length);
            out.write(payload);
        }
        return bytes.toByteArray();
    }
}

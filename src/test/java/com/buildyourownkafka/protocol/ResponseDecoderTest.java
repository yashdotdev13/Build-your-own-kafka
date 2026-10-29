package com.buildyourownkafka.protocol;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class ResponseDecoderTest {

    @Test
    void shouldDecodeValidResponse() throws IOException {
        byte[] payload = new byte[]{1, 2, 3};
        byte[] frame = responseFrame(42, Response.SUCCESS, payload);

        Response decoded = decoder(frame).decode();

        assertNotNull(decoded);
        assertEquals(42, decoded.correlationId());
        assertEquals(Response.SUCCESS, decoded.status());
        assertArrayEquals(payload, decoded.payload());
    }

    @Test
    void shouldRejectResponseWithTrailingBytes() throws IOException {
        byte[] frame = rawResponseFrame(
                42, Response.SUCCESS, 0, new byte[]{99});

        assertThrows(IOException.class, () -> decoder(frame).decode());
    }

    @Test
    void shouldRejectResponseWhenDeclaredPayloadExceedsAvailableBytes()
            throws IOException {
        byte[] frame = rawResponseFrame(
                42, Response.SUCCESS, 2, new byte[]{10});

        assertThrows(IOException.class, () -> decoder(frame).decode());
    }

    @Test
    void shouldRejectIncompleteResponseHeader() throws IOException {
        byte[] frame = rawFrame(new byte[]{0, 0, 0, 42, 0, 0, 0, 0});

        assertThrows(IOException.class, () -> decoder(frame).decode());
    }

    @Test
    void shouldRejectInvalidResponseStatus() throws IOException {
        byte[] frame = responseFrame(
                42,
                99, // Invalid status: only SUCCESS (0) and ERROR (1) are defined
                new byte[]{1, 2, 3}
        );

        assertThrows(
                IOException.class,
                () -> decoder(frame).decode()
        );
    }

    private static ResponseDecoder decoder(byte[] bytes) {
        return new ResponseDecoder(
                new DataInputStream(new ByteArrayInputStream(bytes)));
    }

    private static byte[] responseFrame(
            int correlationId, int status, byte[] payload) throws IOException {
        return rawResponseFrame(
                correlationId, status, payload.length, payload);
    }

    private static byte[] rawResponseFrame(
            int correlationId, int status,
            int declaredLength, byte[] payload) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(correlationId);
            out.writeInt(status);
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

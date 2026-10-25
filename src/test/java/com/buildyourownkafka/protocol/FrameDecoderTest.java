package com.buildyourownkafka.protocol;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class FrameDecoderTest {

    @Test
    void shouldReturnNullWhenEofOccursBeforeNewFrame() throws IOException {
        FrameDecoder decoder = decoder(new byte[0]);

        assertNull(decoder.decode());
    }

    @Test
    void shouldRejectPartialLengthPrefix() {
        FrameDecoder decoder = decoder(new byte[]{0, 0});

        assertThrows(
                EOFException.class,
                decoder::decode
        );
    }

    @Test
    void shouldRejectPayloadShorterThanDeclaredLength() {
        byte[] bytes = ByteBuffer.allocate(6)
                .putInt(5)
                .put((byte) 10)
                .put((byte) 20)
                .array();

        FrameDecoder decoder = decoder(bytes);

        assertThrows(
                EOFException.class,
                decoder::decode
        );
    }

    @Test
    void shouldDecodeMultipleConsecutiveFrames() throws IOException {
        byte[] bytes = ByteBuffer.allocate(11)
                .putInt(3)
                .put(new byte[]{1, 2, 3})
                .putInt(0)
                .array();

        FrameDecoder decoder = decoder(bytes);

        Frame first = decoder.decode();
        Frame second = decoder.decode();

        assertNotNull(first);
        assertNotNull(second);
        assertNull(decoder.decode());
    }

    private FrameDecoder decoder(byte[] bytes) {
        return new FrameDecoder(
                new DataInputStream(new ByteArrayInputStream(bytes))
        );
    }
}


package com.buildyourownkafka.protocol;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class FetchResponsePayloadTest {

    @Test
    void shouldRejectNegativeRecordCount() throws IOException {
        byte[] payload = payloadWith(out -> out.writeInt(-1));

        assertThrows(
                IllegalArgumentException.class,
                () -> FetchResponsePayload.decode(payload)
        );
    }

    @Test
    void shouldRejectExcessiveDeclaredRecordCount() throws IOException {
        byte[] payload = payloadWith(
                out -> out.writeInt(Integer.MAX_VALUE)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> FetchResponsePayload.decode(payload)
        );
    }

    @Test
    void shouldRejectNegativeRecordOffset() throws IOException {
        byte[] payload = payloadWith(out -> {
            out.writeInt(1);
            out.writeLong(-1L);
        });

        assertThrows(
                IllegalArgumentException.class,
                () -> FetchResponsePayload.decode(payload)
        );
    }

    @Test
    void shouldRejectNegativeValueLength() throws IOException {
        byte[] payload = payloadWith(out -> {
            out.writeInt(1);
            out.writeLong(0L);
            out.writeInt(-1);
        });

        assertThrows(
                IllegalArgumentException.class,
                () -> FetchResponsePayload.decode(payload)
        );
    }

    @Test
    void shouldRejectValueLengthExceedingRemainingPayload()
            throws IOException {
        byte[] payload = payloadWith(out -> {
            out.writeInt(1);
            out.writeLong(0L);
            out.writeInt(Integer.MAX_VALUE);
        });

        assertThrows(
                IllegalArgumentException.class,
                () -> FetchResponsePayload.decode(payload)
        );
    }

    @Test
    void shouldRejectTrailingBytes() throws IOException {
        byte[] payload = payloadWith(out -> {
            out.writeInt(0);
            out.writeByte(42);
        });

        assertThrows(
                IllegalArgumentException.class,
                () -> FetchResponsePayload.decode(payload)
        );
    }

    @Test
    void shouldRoundTripValidPayload() {
        FetchResponsePayload original =
                new FetchResponsePayload(java.util.List.of(
                        new com.buildyourownkafka.broker.Record(
                                5L, new byte[]{10, 20, 30}
                        )
                ));

        FetchResponsePayload decoded =
                FetchResponsePayload.decode(original.encode());

        assertEquals(1, decoded.records().size());
        assertEquals(5L, decoded.records().get(0).offset());
        assertArrayEquals(
                new byte[]{10, 20, 30},
                decoded.records().get(0).value()
        );
    }

    @Test
    void shouldAcceptEmptyRecordList() {
        FetchResponsePayload original =
                new FetchResponsePayload(java.util.List.of());

        FetchResponsePayload decoded =
                FetchResponsePayload.decode(original.encode());

        assertTrue(decoded.records().isEmpty());
    }

    @FunctionalInterface
    private interface PayloadWriter {
        void write(DataOutputStream out) throws IOException;
    }

    private static byte[] payloadWith(PayloadWriter writer)
            throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        try (DataOutputStream out = new DataOutputStream(buffer)) {
            writer.write(out);
        }

        return buffer.toByteArray();
    }
}

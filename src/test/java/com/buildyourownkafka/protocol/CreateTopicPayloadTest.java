
package com.buildyourownkafka.protocol;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class CreateTopicPayloadTest {

    @Test
    void shouldDecodeValidPayload() {
        CreateTopicPayload original =
                new CreateTopicPayload("orders", 3);

        CreateTopicPayload decoded =
                CreateTopicPayload.decode(original.encode());

        assertEquals(original, decoded);
    }

    @Test
    void shouldRejectTrailingBytes() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.write(new CreateTopicPayload("orders", 3).encode());
        bytes.write(99);

        assertThrows(
                IllegalArgumentException.class,
                () -> CreateTopicPayload.decode(bytes.toByteArray())
        );
    }

    @Test
    void shouldRejectNegativeTopicLength() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(-1);
        }

        assertThrows(
                IllegalArgumentException.class,
                () -> CreateTopicPayload.decode(bytes.toByteArray())
        );
    }

    @Test
    void shouldRejectTopicLengthExceedingLimit() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(256);
        }

        assertThrows(
                IllegalArgumentException.class,
                () -> CreateTopicPayload.decode(bytes.toByteArray())
        );
    }

    @Test
    void shouldRejectZeroPartitionCount() {
        CreateTopicPayload payload = new CreateTopicPayload("orders", 1);
        byte[] bytes = payload.encode();

        // The final four bytes encode the partition count.
        bytes[bytes.length - 4] = 0;
        bytes[bytes.length - 3] = 0;
        bytes[bytes.length - 2] = 0;
        bytes[bytes.length - 1] = 0;

        assertThrows(
                IllegalArgumentException.class,
                () -> CreateTopicPayload.decode(bytes)
        );
    }
}


package com.buildyourownkafka.protocol;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class FetchOffsetPayloadTest {

    @Test
    void shouldDecodeValidPayload() {
        FetchOffsetPayload original =
                new FetchOffsetPayload("group-1", "orders", 0);

        FetchOffsetPayload decoded =
                FetchOffsetPayload.decode(original.encode());

        assertEquals(original, decoded);
    }

    @Test
    void shouldRejectTrailingBytes() throws IOException {
        FetchOffsetPayload original =
                new FetchOffsetPayload("group-1", "orders", 0);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.write(original.encode());
        bytes.write(99);

        assertThrows(
                RuntimeException.class,
                () -> FetchOffsetPayload.decode(bytes.toByteArray())
        );
    }

    @Test
    void shouldRejectNegativeGroupIdLength() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(-1);
        }

        assertThrows(
                RuntimeException.class,
                () -> FetchOffsetPayload.decode(bytes.toByteArray())
        );
    }

    @Test
    void shouldRejectNegativeTopicLength() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (DataOutputStream output = new DataOutputStream(bytes)) {
            byte[] groupId = "group-1".getBytes(StandardCharsets.UTF_8);

            output.writeInt(groupId.length);
            output.write(groupId);
            output.writeInt(-1);
        }

        assertThrows(
                RuntimeException.class,
                () -> FetchOffsetPayload.decode(bytes.toByteArray())
        );
    }
}

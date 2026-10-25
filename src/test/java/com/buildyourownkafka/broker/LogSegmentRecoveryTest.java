package com.buildyourownkafka.broker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class LogSegmentRecoveryTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldRecoverCompleteRecordsAfterReopeningSegment() {
        Path segmentFile = tempDir.resolve("segment-0.log");
        LogSegment original = new LogSegment(segmentFile);
        original.append(new Record(0, "order-101".getBytes()));
        original.append(new Record(1, "order-102".getBytes()));
        LogSegment recovered = new LogSegment(segmentFile);
        assertEquals(2, recovered.recordCount());
        assertEquals(2L, recovered.nextOffset());
        assertEquals("order-101", new String(recovered.read(0).value()));
        assertEquals("order-102", new String(recovered.read(1).value()));
    }

    @Test
    void shouldRejectIncompleteRecordHeaderDuringRecovery() throws IOException {
        Path segmentFile = tempDir.resolve("segment-0.log");
        Files.write(segmentFile, new byte[] {0, 0, 0, 0});
        assertThrows(RuntimeException.class, () -> new LogSegment(segmentFile));
    }

    @Test
    void shouldRejectIncompleteRecordPayloadDuringRecovery() throws IOException {
        Path segmentFile = tempDir.resolve("segment-0.log");
        byte[] incompleteRecord = {
                0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 5,
                'o', 'k'
        };
        Files.write(segmentFile, incompleteRecord);
        assertThrows(RuntimeException.class, () -> new LogSegment(segmentFile));
    }


    @Test
    void shouldRejectNegativeRecordLengthDuringRecovery() throws IOException {
        Path segmentFile = tempDir.resolve("segment-0.log");

        byte[] invalidRecord = {
                0, 0, 0, 0, 0, 0, 0, 0, // offset = 0
                -1, -1, -1, -1            // length = -1
        };

        Files.write(segmentFile, invalidRecord);

        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> new LogSegment(segmentFile)
        );

        assertTrue(exception.getMessage().contains(segmentFile.toString())
                || exception.getCause().getMessage().contains(segmentFile.toString()));
    }


    @Test
    void shouldRejectUnreasonablyLargeRecordLengthDuringRecovery()
            throws IOException {
        Path segmentFile = tempDir.resolve("segment-0.log");

        byte[] invalidRecord = {
                0, 0, 0, 0, 0, 0, 0, 0, // offset = 0
                127, -1, -1, -1          // length = Integer.MAX_VALUE
        };

        Files.write(segmentFile, invalidRecord);

        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> new LogSegment(segmentFile)
        );

        assertTrue(
                exception.getCause().getMessage().contains("record value length"),
                "Expected record length validation to reject the oversized length"
        );
    }


    @Test
    void shouldIncludeSegmentPathWhenRecordPayloadIsTruncated()
            throws IOException {
        Path segmentFile = tempDir.resolve("segment-0.log");

        byte[] incompleteRecord = {
                0, 0, 0, 0, 0, 0, 0, 0, // offset = 0
                0, 0, 0, 5,               // length = 5
                'o', 'k'                   // only 2 payload bytes
        };

        Files.write(segmentFile, incompleteRecord);

        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> new LogSegment(segmentFile)
        );

        assertTrue(exception.getMessage().contains(segmentFile.toString())
                || exception.getCause().getMessage().contains(segmentFile.toString()));
    }



    @Test
    void shouldRejectCorruptionAfterAValidRecordDuringRecovery()
            throws IOException {
        Path segmentFile = tempDir.resolve("segment-0.log");

        LogSegment segment = new LogSegment(segmentFile);
        segment.append(new Record(0, "valid".getBytes()));

        // Append a second record header with an invalid negative length.
        Files.write(
                segmentFile,
                new byte[] {
                        0, 0, 0, 0, 0, 0, 0, 1, // offset = 1
                        -1, -1, -1, -1           // length = -1
                },
                java.nio.file.StandardOpenOption.APPEND
        );

        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> new LogSegment(segmentFile)
        );

        assertTrue(
                exception.getCause().getMessage().contains(segmentFile.toString()),
                "Corruption error should identify the affected segment"
        );
    }


}

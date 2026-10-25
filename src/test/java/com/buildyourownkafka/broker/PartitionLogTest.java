package com.buildyourownkafka.broker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class PartitionLogTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldAppendAndReadRecordsByOffset() {
        PartitionLog log = new PartitionLog(tempDir);
        log.append(new Record(0, bytes("order-101")));
        log.append(new Record(1, bytes("order-102")));
        assertEquals(0L, log.read(0).offset());
        assertEquals("order-101", value(log.read(0)));
        assertEquals(1L, log.read(1).offset());
        assertEquals("order-102", value(log.read(1)));
        assertNull(log.read(2));
    }

    @Test
    void shouldAdvanceNextOffsetAfterAppending() {
        PartitionLog log = new PartitionLog(tempDir);
        assertEquals(0L, log.nextOffset());
        log.append(new Record(0, bytes("order-101")));
        assertEquals(1L, log.nextOffset());
        log.append(new Record(1, bytes("order-102")));
        assertEquals(2L, log.nextOffset());
    }

    @Test
    void shouldReadMultipleRecordsFromOffset() {
        PartitionLog log = new PartitionLog(tempDir);

        log.append(new Record(0, bytes("order-101")));
        log.append(new Record(1, bytes("order-102")));
        log.append(new Record(2, bytes("order-103")));
        List<Record> records = log.readFrom(1, 2);
        assertEquals(2, records.size());
        assertEquals(1L, records.get(0).offset());
        assertEquals("order-102", value(records.get(0)));
        assertEquals(2L, records.get(1).offset());
        assertEquals("order-103", value(records.get(1)));
    }

    @Test
    void shouldReturnEmptyListWhenReadingBeyondEnd() {
        PartitionLog log = new PartitionLog(tempDir);
        log.append(new Record(0, bytes("order-101")));
        assertTrue(log.readFrom(1, 10).isEmpty());
    }

    @Test
    void shouldRejectNegativeReadOffset() {
        PartitionLog log = new PartitionLog(tempDir);
        assertThrows(IllegalArgumentException.class, () -> log.read(-1));
        assertThrows(IllegalArgumentException.class, () -> log.readFrom(-1, 10));
    }

    @Test
    void shouldRejectNonPositiveReadLimit() {
        PartitionLog log = new PartitionLog(tempDir);
        assertThrows(IllegalArgumentException.class, () -> log.readFrom(0, 0));
        assertThrows(IllegalArgumentException.class, () -> log.readFrom(0, -1));
    }

    @Test
    void shouldRejectNullRecord() {
        PartitionLog log = new PartitionLog(tempDir);
        assertThrows(IllegalArgumentException.class, () -> log.append(null));
    }

    @Test
    void shouldRecoverRecordsAfterReopeningLog() {
        PartitionLog first = new PartitionLog(tempDir);
        first.append(new Record(0, bytes("order-101")));
        first.append(new Record(1, bytes("order-102")));
        PartitionLog reopened = new PartitionLog(tempDir);
        assertEquals(2L, reopened.nextOffset());
        assertEquals("order-101", value(reopened.read(0)));
        assertEquals("order-102", value(reopened.read(1)));
    }

    @Test
    void shouldRotateSegmentAfterThreeRecords() throws IOException {
        PartitionLog log = new PartitionLog(tempDir);
        log.append(new Record(0, bytes("order-101")));
        log.append(new Record(1, bytes("order-102")));
        log.append(new Record(2, bytes("order-103")));
        log.append(new Record(3, bytes("order-104")));
        assertTrue(Files.exists(tempDir.resolve("segment-0.log")));
        assertTrue(Files.exists(tempDir.resolve("segment-3.log")));
        assertEquals(4L, log.nextOffset());
        assertEquals("order-101", value(log.read(0)));
        assertEquals("order-104", value(log.read(3)));
    }
    @Test
    void shouldPreserveExistingRecordsWhenSegmentCreationFails() {
        AtomicBoolean failNextSegment = new AtomicBoolean(true);

        PartitionLog log = new PartitionLog(tempDir, path -> {
            if (path.getFileName().toString().equals("segment-3.log")
                    && failNextSegment.getAndSet(false)) {
                throw new RuntimeException(
                        "Simulated segment creation failure"
                );
            }

            return new LogSegment(path);
        });

        log.append(new Record(0, bytes("order-101")));
        log.append(new Record(1, bytes("order-102")));
        log.append(new Record(2, bytes("order-103")));

        assertThrows(
                RuntimeException.class,
                () -> log.append(new Record(3, bytes("order-104")))
        );

        // The existing records and offsets must remain intact.
        assertEquals(3, log.nextOffset());
        assertEquals("order-101", value(log.read(0)));
        assertEquals("order-102", value(log.read(1)));
        assertEquals("order-103", value(log.read(2)));

        // The next attempt should successfully create the segment.
        log.append(new Record(3, bytes("order-104")));

        assertEquals(4, log.nextOffset());
        assertEquals("order-104", value(log.read(3)));
    }

    @Test
    void shouldRecoverWhenSegmentCreationLeavesAnEmptyFile() {
        AtomicBoolean failOnce = new AtomicBoolean(true);

        PartitionLog log = new PartitionLog(tempDir, path -> {
            if (path.getFileName().toString().equals("segment-3.log")
                    && failOnce.getAndSet(false)) {
                try {
                    Files.createFile(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                throw new RuntimeException(
                        "Simulated failure after file creation"
                );
            }

            return new LogSegment(path);
        });

        log.append(new Record(0, bytes("order-101")));
        log.append(new Record(1, bytes("order-102")));
        log.append(new Record(2, bytes("order-103")));

        assertThrows(
                RuntimeException.class,
                () -> log.append(new Record(3, bytes("order-104")))
        );

        assertEquals(3, log.nextOffset());
        assertTrue(Files.exists(tempDir.resolve("segment-3.log")));

        // Simulate a process restart using the persisted files.
        PartitionLog recoveredLog = new PartitionLog(tempDir);

        assertEquals(3, recoveredLog.nextOffset());
        assertEquals("order-101", value(recoveredLog.read(0)));
        assertEquals("order-102", value(recoveredLog.read(1)));
        assertEquals("order-103", value(recoveredLog.read(2)));

        recoveredLog.append(new Record(3, bytes("order-104")));

        assertEquals(4, recoveredLog.nextOffset());
        assertEquals("order-104", value(recoveredLog.read(3)));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String value(Record record) {
        return new String(record.value(), StandardCharsets.UTF_8);
    }
}

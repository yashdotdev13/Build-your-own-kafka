package com.buildyourownkafka.broker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PartitionLogRecoveryTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldRecoverAllRecordsAcrossMultipleSegments() {
        PartitionLog original = new PartitionLog(tempDir);

        for (int i = 0; i < 9; i++) {
            original.append(record(i, "message-" + i));
        }

        PartitionLog recovered = new PartitionLog(tempDir);

        assertEquals(9L, recovered.nextOffset());

        for (int i = 0; i < 9; i++) {
            Record actual = recovered.read(i);

            assertNotNull(actual, "Missing record at offset " + i);
            assertEquals(i, actual.offset());
            assertEquals("message-" + i, value(actual));
        }
    }

    @Test
    void shouldCreateThreeSegmentsForNineRecords() throws Exception {
        PartitionLog log = new PartitionLog(tempDir);

        for (int i = 0; i < 9; i++) {
            log.append(record(i, "message-" + i));
        }

        List<String> segmentNames;

        try (var files = Files.list(tempDir)) {
            segmentNames = files
                    .map(path -> path.getFileName().toString())
                    .filter(name -> name.startsWith("segment-"))
                    .sorted()
                    .toList();
        }

        assertEquals(
                List.of("segment-0.log", "segment-3.log", "segment-6.log"),
                segmentNames
        );
    }

    @Test
    void shouldPreserveNextOffsetAfterReopeningRotatedLog() {
        PartitionLog original = new PartitionLog(tempDir);

        for (int i = 0; i < 9; i++) {
            original.append(record(i, "message-" + i));
        }

        PartitionLog recovered = new PartitionLog(tempDir);

        assertEquals(9L, recovered.nextOffset());

        recovered.append(record(9, "message-9"));

        assertEquals(10L, recovered.nextOffset());
        assertEquals("message-9", value(recovered.read(9)));
    }

    @Test
    void shouldPreserveNextOffsetWhenTrailingSegmentIsEmpty() throws Exception {
        PartitionLog original = new PartitionLog(tempDir);

        for (int i = 0; i < 9; i++) {
            original.append(record(i, "message-" + i));
        }

        // Simulate an empty segment left behind by an interrupted operation.
        Files.createFile(tempDir.resolve("segment-9.log"));

        PartitionLog recovered = new PartitionLog(tempDir);

        assertEquals(9L, recovered.nextOffset());

        recovered.append(record(recovered.nextOffset(), "message-9"));

        assertEquals(10L, recovered.nextOffset());
        assertEquals("message-9", value(recovered.read(9)));
    }

    @Test
    void shouldRejectDuplicateOffset() {
        PartitionLog log = new PartitionLog(tempDir);

        log.append(record(0, "first"));

        assertThrows(
                IllegalArgumentException.class,
                () -> log.append(record(0, "duplicate"))
        );

        assertEquals(1L, log.nextOffset());
        assertEquals("first", value(log.read(0)));
    }

    @Test
    void shouldRejectOutOfOrderOffset() {
        PartitionLog log = new PartitionLog(tempDir);

        log.append(record(0, "first"));

        assertThrows(
                IllegalArgumentException.class,
                () -> log.append(record(2, "out-of-order"))
        );

        assertEquals(1L, log.nextOffset());
        assertEquals("first", value(log.read(0)));
        assertNull(log.read(2));
    }

    @Test
    void shouldRejectNonSequentialOffsetsDuringRecovery() {
        Path segmentFile = tempDir.resolve("segment-0.log");
        LogSegment segment = new LogSegment(segmentFile);

        segment.append(record(0, "first"));
        segment.append(record(2, "gap"));

        assertThrows(
                RuntimeException.class,
                () -> new PartitionLog(tempDir)
        );
    }

    @Test
    void shouldRejectDuplicateOffsetsDuringRecovery() {
        LogSegment segment = new LogSegment(
                tempDir.resolve("segment-0.log")
        );

        segment.append(record(0, "first"));
        segment.append(record(0, "duplicate"));

        assertThrows(
                RuntimeException.class,
                () -> new PartitionLog(tempDir)
        );
    }

    @Test
    void shouldRejectOutOfOrderOffsetsAcrossSegments() {
        LogSegment firstSegment = new LogSegment(
                tempDir.resolve("segment-0.log")
        );

        firstSegment.append(record(0, "first"));
        firstSegment.append(record(1, "second"));

        LogSegment secondSegment = new LogSegment(
                tempDir.resolve("segment-2.log")
        );

        secondSegment.append(record(2, "third"));
        secondSegment.append(record(1, "out-of-order"));

        assertThrows(
                RuntimeException.class,
                () -> new PartitionLog(tempDir)
        );
    }

    private static Record record(long offset, String value) {
        return new Record(
                offset,
                value.getBytes(StandardCharsets.UTF_8)
        );
    }

    private static String value(Record record) {
        return new String(record.value(), StandardCharsets.UTF_8);
    }
}

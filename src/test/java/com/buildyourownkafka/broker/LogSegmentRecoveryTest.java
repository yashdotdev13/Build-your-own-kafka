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
}

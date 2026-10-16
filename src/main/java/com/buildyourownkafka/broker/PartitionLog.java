package com.buildyourownkafka.broker;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class PartitionLog {

    private static final long MAX_RECORDS_PER_SEGMENT = 3;

    private final Path directory;
    private final List<LogSegment> segments = new ArrayList<>();

    private LogSegment activeSegment;

    public PartitionLog(Path directory) {
        if (directory == null) {
            throw new IllegalArgumentException(
                    "Log directory cannot be null"
            );
        }

        this.directory = directory;

        try {
            Files.createDirectories(directory);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to initialize partition log", e
            );
        }

        migrateLegacyLog();
        loadSegments();
    }

    public synchronized void append(Record record) {
        if (record == null) {
            throw new IllegalArgumentException(
                    "Record cannot be null"
            );
        }

        long expectedOffset = nextOffset();

        if (record.offset() != expectedOffset) {
            throw new IllegalArgumentException(
                    "Invalid record offset: expected "
                            + expectedOffset
                            + " but received "
                            + record.offset()
            );
        }

        if (activeSegment.recordCount() >= MAX_RECORDS_PER_SEGMENT) {
            createNewSegment(record.offset());
        }

        activeSegment.append(record);
    }

    public synchronized Record read(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException(
                    "Offset cannot be negative"
            );
        }

        for (LogSegment segment : segments) {
            Record record = segment.read(offset);

            if (record != null) {
                return record;
            }
        }

        return null;
    }

    public synchronized List<Record> readFrom(
            long offset,
            int maxRecords
    ) {
        if (offset < 0) {
            throw new IllegalArgumentException(
                    "Offset cannot be negative"
            );
        }

        if (maxRecords <= 0) {
            throw new IllegalArgumentException(
                    "Max records must be greater than zero"
            );
        }

        List<Record> records = new ArrayList<>();

        for (long currentOffset = offset;
             records.size() < maxRecords;
             currentOffset++) {

            Record record = read(currentOffset);

            if (record == null) {
                break;
            }

            records.add(record);
        }

        return records;
    }

    public synchronized long nextOffset() {
        long nextOffset = 0;

        for (LogSegment segment : segments) {
            nextOffset = Math.max(
                    nextOffset,
                    segment.nextOffset()
            );
        }

        return nextOffset;
    }

    private void migrateLegacyLog() {
        Path legacyFile = directory.resolve("partition.log");

        if (!Files.exists(legacyFile)) {
            return;
        }

        Path firstSegment = directory.resolve("segment-0.log");

        try {
            if (Files.exists(firstSegment)) {
                throw new IllegalStateException(
                        "Both legacy and segmented log exist: "
                                + directory
                );
            }

            Files.move(legacyFile, firstSegment);

            System.out.println(
                    "Migrated legacy log: "
                            + legacyFile.getFileName()
                            + " -> "
                            + firstSegment.getFileName()
            );

        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to migrate legacy log: " + legacyFile, e
            );
        }
    }

    private void loadSegments() {
        try {
            List<Path> files;

            try (var paths = Files.list(directory)) {
                files = paths
                        .filter(Files::isRegularFile)
                        .filter(path -> {
                            String name = path.getFileName().toString();

                            return name.startsWith("segment-")
                                    && name.endsWith(".log");
                        })
                        .sorted(
                                Comparator.comparingLong(
                                        this::extractSegmentId
                                )
                        )
                        .toList();
            }

            if (files.isEmpty()) {
                createNewSegment(0);
                return;
            }

            long expectedOffset = 0;

            for (Path file : files) {
                LogSegment segment = new LogSegment(file);

                expectedOffset = segment.validateOffsets(
                        expectedOffset
                );

                segments.add(segment);
            }

            activeSegment = segments.get(segments.size() - 1);

        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to load log segments", e
            );
        }
    }

    private void createNewSegment(long baseOffset) {
        Path segmentFile = directory.resolve(
                "segment-" + baseOffset + ".log"
        );

        LogSegment segment = new LogSegment(segmentFile);

        segments.add(segment);
        activeSegment = segment;

        System.out.println(
                "Created log segment: " + segmentFile.getFileName()
        );
    }

    private long extractSegmentId(Path path) {
        String fileName = path.getFileName().toString();

        String id = fileName.substring(
                "segment-".length(),
                fileName.length() - ".log".length()
        );

        return Long.parseLong(id);
    }
}

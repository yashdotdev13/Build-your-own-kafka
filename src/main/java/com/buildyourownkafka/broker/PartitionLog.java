package com.buildyourownkafka.broker;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PartitionLog {

    private static final long MAX_RECORDS_PER_SEGMENT = 3;

    private static final Pattern SEGMENT_FILE_PATTERN =
            Pattern.compile("segment-(0|[1-9][0-9]*)\\.log");

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
            List<Path> entries;

            try (var paths = Files.list(directory)) {
                entries = paths.toList();
            }

            List<Path> validSegmentFiles = new ArrayList<>();

            for (Path entry : entries) {
                String fileName = entry.getFileName().toString();

                // The legacy file should already have been migrated.
                if (fileName.equals("partition.log")) {
                    throw new IllegalStateException(
                            "Legacy log was not migrated: " + entry
                    );
                }

                Matcher matcher = SEGMENT_FILE_PATTERN.matcher(fileName);

                if (!matcher.matches()) {
                    throw new IllegalStateException(
                            "Unexpected entry in partition directory: " + entry
                    );
                }

                if (!Files.isRegularFile(entry)) {
                    throw new IllegalStateException(
                            "Log segment must be a regular file: " + entry
                    );
                }

                try {
                    Long.parseLong(matcher.group(1));
                } catch (NumberFormatException e) {
                    throw new IllegalStateException(
                            "Log segment ID is outside the supported range: "
                                    + entry,
                            e
                    );
                }

                validSegmentFiles.add(entry);
            }

            validSegmentFiles.sort(
                    Comparator.comparingLong(this::extractSegmentId)
            );

            if (validSegmentFiles.isEmpty()) {
                createNewSegment(0);
                return;
            }

            long expectedOffset = 0;

            for (Path file : validSegmentFiles) {
                LogSegment segment = new LogSegment(file);
                expectedOffset = segment.validateOffsets(expectedOffset);
                segments.add(segment);
            }

            activeSegment = segments.get(segments.size() - 1);

        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to load log segments from directory: " + directory,
                    e
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

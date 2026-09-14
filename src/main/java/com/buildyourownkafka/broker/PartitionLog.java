package com.buildyourownkafka.broker;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class PartitionLog {

    private static final long MAX_RECORDS_PER_SEGMENT = 3;

    private final Path directory;

    private final List<LogSegment> segments =
            new ArrayList<>();

    private LogSegment activeSegment;

    public PartitionLog(Path directory) {

        if (directory == null) {
            throw new IllegalArgumentException(
                    "Log directory cannot be null");
        }

        this.directory = directory;

        try {
            Files.createDirectories(directory);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to initialize partition log", e);
        }

        loadSegments();
    }

    public synchronized void append(Record record) {

        if (record == null) {
            throw new IllegalArgumentException(
                    "Record cannot be null");
        }

        if (activeSegment.recordCount()
                >= MAX_RECORDS_PER_SEGMENT) {

            createNewSegment(record.offset());
        }

        activeSegment.append(record);
    }

    public synchronized Record read(long offset) {

        if (offset < 0) {
            throw new IllegalArgumentException(
                    "Offset cannot be negative");
        }

        for (LogSegment segment : segments) {

            Record record =
                    segment.read(offset);

            if (record != null) {
                return record;
            }
        }

        return null;
    }

    public synchronized long nextOffset() {

        if (segments.isEmpty()) {
            return 0;
        }

        return activeSegment.nextOffset();
    }

    private void loadSegments() {

        try {

            List<Path> files;

            try (var paths = Files.list(directory)) {

                files =
                        paths
                                .filter(Files::isRegularFile)
                                .filter(path ->
                                        path.getFileName()
                                                .toString()
                                                .startsWith("segment-"))
                                .filter(path ->
                                        path.getFileName()
                                                .toString()
                                                .endsWith(".log"))
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

            for (Path file : files) {

                segments.add(
                        new LogSegment(file)
                );
            }

            activeSegment =
                    segments.get(
                            segments.size() - 1
                    );

        } catch (Exception e) {

            throw new RuntimeException(
                    "Failed to load log segments", e);
        }
    }

    private void createNewSegment(long baseOffset) {

        Path segmentFile =
                directory.resolve(
                        "segment-" + baseOffset + ".log"
                );

        LogSegment segment =
                new LogSegment(segmentFile);

        segments.add(segment);

        activeSegment = segment;

        System.out.println(
                "Created log segment: "
                        + segmentFile.getFileName()
        );
    }

    private long extractSegmentId(Path path) {

        String fileName =
                path.getFileName()
                        .toString();

        String id =
                fileName
                        .substring(
                                "segment-".length(),
                                fileName.length()
                                        - ".log".length()
                        );

        return Long.parseLong(id);
    }
}
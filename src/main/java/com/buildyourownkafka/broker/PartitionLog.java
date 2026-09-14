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
                    "Log directory cannot be null"
            );
        }

        this.directory = directory;

        try {

            Files.createDirectories(directory);

        } catch (Exception e) {

            throw new RuntimeException(
                    "Failed to initialize partition log",
                    e
            );
        }

        // Migrate old single-file log if it exists
        migrateLegacyLog();

        // Load segmented logs
        loadSegments();
    }

    public synchronized void append(Record record) {

        if (record == null) {
            throw new IllegalArgumentException(
                    "Record cannot be null"
            );
        }

        /*
         * Rotate the segment when the current segment
         * reaches the maximum number of records.
         */
        if (activeSegment.recordCount()
                >= MAX_RECORDS_PER_SEGMENT) {

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

        /*
         * For now we scan segments sequentially.
         *
         * Later we can optimize this by determining
         * the correct segment directly from the offset.
         */
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

    /**
     * Migrates the old single-file storage format:
     *
     *     partition.log
     *
     * into the new segmented format:
     *
     *     segment-0.log
     *
     * The record format is unchanged, so we only need
     * to rename/move the file.
     */
    private void migrateLegacyLog() {

        Path legacyFile =
                directory.resolve("partition.log");

        if (!Files.exists(legacyFile)) {
            return;
        }

        Path firstSegment =
                directory.resolve("segment-0.log");

        try {

            /*
             * Safety check:
             *
             * We don't want to silently overwrite an
             * existing segmented log.
             */
            if (Files.exists(firstSegment)) {

                throw new IllegalStateException(
                        "Both legacy and segmented log exist: "
                                + directory
                );
            }

            Files.move(
                    legacyFile,
                    firstSegment
            );

            System.out.println(
                    "Migrated legacy log: "
                            + legacyFile.getFileName()
                            + " -> "
                            + firstSegment.getFileName()
            );

        } catch (Exception e) {

            throw new RuntimeException(
                    "Failed to migrate legacy log: "
                            + legacyFile,
                    e
            );
        }
    }

    /**
     * Loads all segment files from disk.
     *
     * Segment files are ordered using their base offset:
     *
     * segment-0.log
     * segment-3.log
     * segment-6.log
     * ...
     */
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
                                                .startsWith("segment-")
                                )
                                .filter(path ->
                                        path.getFileName()
                                                .toString()
                                                .endsWith(".log")
                                )
                                .sorted(
                                        Comparator.comparingLong(
                                                this::extractSegmentId
                                        )
                                )
                                .toList();
            }

            /*
             * No segment exists.
             *
             * This is a brand-new partition.
             */
            if (files.isEmpty()) {

                createNewSegment(0);

                return;
            }

            /*
             * Load every existing segment.
             */
            for (Path file : files) {

                segments.add(
                        new LogSegment(file)
                );
            }

            /*
             * The last segment becomes the active
             * segment for future writes.
             */
            activeSegment =
                    segments.get(
                            segments.size() - 1
                    );

        } catch (Exception e) {

            throw new RuntimeException(
                    "Failed to load log segments",
                    e
            );
        }
    }

    /**
     * Creates a new segment starting at the supplied
     * base offset.
     */
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

    /**
     * Extracts the base offset from a segment filename.
     *
     * Example:
     *
     * segment-6.log
     *
     * becomes:
     *
     * 6
     */
    private long extractSegmentId(Path path) {

        String fileName =
                path.getFileName()
                        .toString();

        String id =
                fileName.substring(
                        "segment-".length(),
                        fileName.length()
                                - ".log".length()
                );

        return Long.parseLong(id);
    }
}
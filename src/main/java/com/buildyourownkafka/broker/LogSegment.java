package com.buildyourownkafka.broker;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class LogSegment {

    private static final int RECORD_HEADER_SIZE =
            Long.BYTES + Integer.BYTES;

    private final Path file;
    private long recordCount;

    public LogSegment(Path file) {
        if (file == null) {
            throw new IllegalArgumentException(
                    "Segment file cannot be null"
            );
        }

        this.file = file;

        try {
            Path parent = file.getParent();

            if (parent != null) {
                Files.createDirectories(parent);
            }

            if (!Files.exists(file)) {
                Files.createFile(file);
            }

            this.recordCount = countRecords();

        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to initialize log segment: " + file, e
            );
        }
    }

    public synchronized void append(Record record) {
        if (record == null) {
            throw new IllegalArgumentException(
                    "Record cannot be null"
            );
        }

        try (DataOutputStream output = new DataOutputStream(
                Files.newOutputStream(
                        file,
                        StandardOpenOption.APPEND
                ))) {

            output.writeLong(record.offset());
            output.writeInt(record.value().length);
            output.write(record.value());
            output.flush();

            recordCount++;

        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to append record to segment: " + file, e
            );
        }
    }

    public synchronized Record read(long requestedOffset) {
        if (requestedOffset < 0) {
            throw new IllegalArgumentException(
                    "Offset cannot be negative"
            );
        }

        try (DataInputStream input = new DataInputStream(
                Files.newInputStream(file))) {

            while (true) {
                if (input.available() == 0) {
                    return null;
                }

                ensureCompleteHeader(input);

                long offset = input.readLong();
                int valueLength = input.readInt();

                validateRecordLength(valueLength);
                ensureCompletePayload(input, valueLength);

                byte[] value = new byte[valueLength];
                input.readFully(value);

                if (offset == requestedOffset) {
                    return new Record(offset, value);
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to read record from segment: " + file, e
            );
        }
    }

    public synchronized long nextOffset() {
        long nextOffset = 0;

        try (DataInputStream input = new DataInputStream(
                Files.newInputStream(file))) {

            while (true) {
                if (input.available() == 0) {
                    return nextOffset;
                }

                ensureCompleteHeader(input);

                long offset = input.readLong();
                int valueLength = input.readInt();

                validateRecordLength(valueLength);
                ensureCompletePayload(input, valueLength);

                input.skipNBytes(valueLength);

                nextOffset = Math.incrementExact(offset);
            }

        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to determine next offset: " + file, e
            );
        }
    }

    /**
     * Validates sequential offsets in this segment, starting with
     * the expected offset supplied by the caller.
     *
     * Returns the next expected offset after the last record.
     * Empty segments leave the expected offset unchanged.
     */
    public synchronized long validateOffsets(long expectedOffset) {
        if (expectedOffset < 0) {
            throw new IllegalArgumentException(
                    "Expected offset cannot be negative"
            );
        }

        try (DataInputStream input = new DataInputStream(
                Files.newInputStream(file))) {

            while (input.available() > 0) {
                ensureCompleteHeader(input);

                long offset = input.readLong();
                int valueLength = input.readInt();

                validateRecordLength(valueLength);
                ensureCompletePayload(input, valueLength);

                if (offset != expectedOffset) {
                    throw new IllegalStateException(
                            "Invalid offset in segment " + file
                                    + ": expected " + expectedOffset
                                    + " but found " + offset
                    );
                }

                input.skipNBytes(valueLength);
                expectedOffset = Math.incrementExact(expectedOffset);
            }

            return expectedOffset;

        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to validate offsets in segment: " + file, e
            );
        }
    }

    public synchronized long recordCount() {
        return recordCount;
    }

    public Path file() {
        return file;
    }

    private long countRecords() throws IOException {
        long count = 0;

        try (DataInputStream input = new DataInputStream(
                Files.newInputStream(file))) {

            while (true) {
                if (input.available() == 0) {
                    return count;
                }

                ensureCompleteHeader(input);

                input.readLong();
                int valueLength = input.readInt();

                validateRecordLength(valueLength);
                ensureCompletePayload(input, valueLength);

                input.skipNBytes(valueLength);
                count++;
            }
        }
    }

    private void ensureCompleteHeader(DataInputStream input)
            throws IOException {
        if (input.available() < RECORD_HEADER_SIZE) {
            throw new EOFException(
                    "Incomplete record header in log segment: " + file
            );
        }
    }

    private void ensureCompletePayload(
            DataInputStream input,
            int valueLength
    ) throws IOException {
        if (input.available() < valueLength) {
            throw new EOFException(
                    "Incomplete record payload in log segment: " + file
            );
        }
    }

    private void validateRecordLength(int valueLength)
            throws IOException {
        if (valueLength < 0) {
            throw new IOException(
                    "Invalid record value length: " + valueLength
            );
        }
    }
}

package com.buildyourownkafka.broker;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class LogSegment {

    private final Path file;
    private long recordCount;

    public LogSegment(Path file) {
        if (file == null) {
            throw new IllegalArgumentException("Segment file cannot be null");
        }
        this.file = file;
        try {
            Files.createDirectories(file.getParent());
            if (!Files.exists(file)) {
                Files.createFile(file);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize log segment", e);
        }
        this.recordCount = countRecords();
    }

    public synchronized void append(Record record) {
        if (record == null) {
            throw new IllegalArgumentException("Record cannot be null");
        }

        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(file, StandardOpenOption.APPEND))) {
            output.writeLong(record.offset());
            output.writeInt(record.value().length);
            output.write(record.value());
            output.flush();
            recordCount++;
        } catch (IOException e) {
            throw new RuntimeException("Failed to append record to segment", e);
        }
    }

    public synchronized Record read(long requestedOffset) {

        if (requestedOffset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }
        try (DataInputStream input = new DataInputStream(Files.newInputStream(file))) {
            while (true) {
                try {
                    long offset = input.readLong();
                    int valueLength = input.readInt();
                    if (valueLength < 0) {
                        throw new IOException("Invalid record value length: " + valueLength);
                    }
                    byte[] value = input.readNBytes(valueLength);
                    if (value.length != valueLength) {
                        throw new EOFException("Incomplete record in log segment");
                    }
                    if (offset == requestedOffset) {
                        return new Record(offset, value);
                    }
                } catch (EOFException e) {
                    return null;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read record from segment", e);
        }
    }

    public synchronized long nextOffset() {
        long nextOffset = 0;

        try (DataInputStream input = new DataInputStream(Files.newInputStream(file))) {
            while (true) {
                try {
                    long offset = input.readLong();
                    int valueLength = input.readInt();
                    if (valueLength < 0) {
                        throw new IOException("Invalid record value length: " + valueLength);
                    }
                    input.skipNBytes(valueLength);
                    nextOffset = offset + 1;
                } catch (EOFException e) {
                    break;
                }
            }
            return nextOffset;
        } catch (IOException e) {
            throw new RuntimeException("Failed to determine next offset", e);
        }
    }
    public synchronized long recordCount() {
        return recordCount;
    }
    public Path file() {
        return file;
    }

    private long countRecords() {
        long count = 0;
        try (DataInputStream input = new DataInputStream(Files.newInputStream(file))) {
            while (true) {
                try {
                    input.readLong();
                    int valueLength = input.readInt();
                    if (valueLength < 0) {
                        throw new IOException("Invalid record value length: " + valueLength);
                    }
                    input.skipNBytes(valueLength);
                    count++;
                } catch (EOFException e) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to count records in segment", e);
        }
        return count;
    }
}
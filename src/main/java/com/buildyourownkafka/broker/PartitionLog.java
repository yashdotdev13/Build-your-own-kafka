package com.buildyourownkafka.broker;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class PartitionLog {

    private final Path file;

    public PartitionLog(Path file) {
        this.file = file;

        try {
            Files.createDirectories(file.getParent());

            if (!Files.exists(file)) {
                Files.createFile(file);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize partition log", e);
        }
    }

    public synchronized void append(Record record) {

        try (DataOutputStream output =
                     new DataOutputStream(
                             Files.newOutputStream(
                                     file,
                                     java.nio.file.StandardOpenOption.APPEND))) {

            output.writeLong(record.offset());
            output.writeInt(record.value().length);
            output.write(record.value());

            output.flush();

        } catch (IOException e) {
            throw new RuntimeException("Failed to append record to log", e);
        }
    }

    public synchronized Record read(long requestedOffset) {

        if (requestedOffset < 0) {
            throw new IllegalArgumentException(
                    "Offset cannot be negative");
        }

        try (DataInputStream input =
                     new DataInputStream(
                             Files.newInputStream(file))) {

            while (true) {

                try {
                    long offset = input.readLong();

                    int valueLength = input.readInt();

                    if (valueLength < 0) {
                        throw new IOException(
                                "Invalid record value length: " + valueLength);
                    }

                    byte[] value = input.readNBytes(valueLength);

                    if (value.length != valueLength) {
                        throw new EOFException(
                                "Incomplete record in partition log");
                    }

                    if (offset == requestedOffset) {
                        return new Record(offset, value);
                    }

                } catch (EOFException e) {
                    return null;
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to read record from log", e);
        }
    }
}
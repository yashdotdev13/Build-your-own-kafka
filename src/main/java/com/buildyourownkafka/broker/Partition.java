package com.buildyourownkafka.broker;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class Partition {

    private final int id;
    private final PartitionLog log;

    private long nextOffset;

    public Partition(int id, Path logDirectory) {

        if (id < 0) {
            throw new IllegalArgumentException(
                    "Partition id cannot be negative");
        }

        if (logDirectory == null) {
            throw new IllegalArgumentException(
                    "Log directory cannot be null");
        }

        this.id = id;

        this.log =
                new PartitionLog(logDirectory);

        this.nextOffset =
                log.nextOffset();
    }

    public int id() {
        return id;
    }

    public synchronized Record append(byte[] value) {

        if (value == null) {
            throw new IllegalArgumentException(
                    "Record value cannot be null");
        }

        Record record =
                new Record(
                        nextOffset,
                        value
                );

        log.append(record);

        nextOffset++;

        return record;
    }

    public synchronized Record read(long offset) {

        if (offset < 0) {
            return null;
        }

        return log.read(offset);
    }

    public synchronized List<Record> readFrom(long offset) {

        if (offset < 0) {
            throw new IllegalArgumentException(
                    "Offset cannot be negative");
        }

        List<Record> records =
                new ArrayList<>();

        long currentOffset = offset;

        while (currentOffset < nextOffset) {

            Record record =
                    log.read(currentOffset);

            if (record == null) {
                break;
            }

            records.add(record);

            currentOffset++;
        }

        return List.copyOf(records);
    }

    public synchronized long nextOffset() {
        return nextOffset;
    }

    public synchronized int size() {
        return (int) nextOffset;
    }
}
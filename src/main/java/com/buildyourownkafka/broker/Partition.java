package com.buildyourownkafka.broker;

import java.nio.file.Path;
import java.util.List;

public class Partition {

    private final int id;
    private final PartitionLog log;

    private long nextOffset;

    public Partition(int id, Path logFile) {

        if (id < 0) {
            throw new IllegalArgumentException(
                    "Partition id cannot be negative");
        }

        if (logFile == null) {
            throw new IllegalArgumentException(
                    "Log file cannot be null");
        }

        this.id = id;
        this.log = new PartitionLog(logFile);
        this.nextOffset = log.nextOffset();
    }

    public int id() {
        return id;
    }

    public synchronized Record append(byte[] value) {

        if (value == null) {
            throw new IllegalArgumentException(
                    "Record value cannot be null");
        }

        Record record = new Record(nextOffset, value);

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

        List<Record> records = new java.util.ArrayList<>();

        long currentOffset = offset;

        while (currentOffset < nextOffset) {

            Record record = log.read(currentOffset);

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
package com.buildyourownkafka.broker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Partition {

    private final int id;
    private final List<Record> records = new ArrayList<>();

    public Partition(int id) {
        if (id < 0) {
            throw new IllegalArgumentException("Partition id cannot be negative");
        }
        this.id = id;
    }
    public int id() {
        return id;
    }

    public synchronized Record append(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("Record value cannot be null");
        }

        long offset = records.size();
        Record record = new Record(offset, value);
        records.add(record);
        return record;
    }

    public synchronized Record read(long offset) {
        if (offset < 0 || offset >= records.size()) {
            return null;
        }

        return records.get((int) offset);
    }
    public synchronized List<Record> readFrom(long offset) {

        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }
        if (offset >= records.size()) {
            return Collections.emptyList();
        }
        return List.copyOf(records.subList((int) offset, records.size()));
    }
    public synchronized long nextOffset() {
        return records.size();
    }
    public synchronized int size() {
        return records.size();
    }
}
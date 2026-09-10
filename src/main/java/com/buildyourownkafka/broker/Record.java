package com.buildyourownkafka.broker;

public record Record(long offset,
                     byte[] value) {

    public Record {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }

        if (value == null) {
            throw new IllegalArgumentException("Record value cannot be null");
        }
    }
}
package com.buildyourownkafka.broker;

public class Partition {

    private final int id;

    public Partition(int id) {
        if (id < 0) {
            throw new IllegalArgumentException(
                    "Partition id cannot be negative"
            );
        }

        this.id = id;
    }

    public int id() {
        return id;
    }
}
package com.buildyourownkafka.client;

public class Consumer {

    private final String topic;
    private final int partition;

    private long currentOffset;

    public Consumer(
            String topic,
            int partition,
            long startingOffset) {

        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException(
                    "Topic cannot be blank");
        }

        if (partition < 0) {
            throw new IllegalArgumentException(
                    "Partition cannot be negative");
        }

        if (startingOffset < 0) {
            throw new IllegalArgumentException(
                    "Starting offset cannot be negative");
        }

        this.topic = topic;
        this.partition = partition;
        this.currentOffset = startingOffset;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public synchronized long currentOffset() {
        return currentOffset;
    }

    public synchronized void advanceOffset(long nextOffset) {

        if (nextOffset < currentOffset) {
            throw new IllegalArgumentException(
                    "Consumer offset cannot move backwards");
        }

        this.currentOffset = nextOffset;
    }
}
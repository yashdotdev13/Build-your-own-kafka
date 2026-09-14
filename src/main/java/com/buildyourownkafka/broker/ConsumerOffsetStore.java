package com.buildyourownkafka.broker;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerOffsetStore {

    private final Path file;
    private final Map<OffsetKey, Long> offsets = new ConcurrentHashMap<>();

    public ConsumerOffsetStore(Path directory) {
        if (directory == null) {
            throw new IllegalArgumentException("Offset directory cannot be null");
        }
        try {
            Files.createDirectories(directory);
            this.file = directory.resolve("offsets.log");
            if (!Files.exists(file)) {
                Files.createFile(file);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize consumer offset store", e);
        }
        loadOffsets();
    }

    public synchronized void commit(String groupId, String topic, int partition, long offset) {

        validate(groupId, topic, partition, offset);
        OffsetKey key = new OffsetKey(groupId, topic, partition);
        offsets.put(key, offset);
        appendToDisk(key, offset);
    }
    public synchronized long fetch(String groupId, String topic, int partition) {
        validateFetchRequest(groupId, topic, partition);
        OffsetKey key = new OffsetKey(groupId, topic, partition);
        return offsets.getOrDefault(key, 0L);
    }
    private void appendToDisk(OffsetKey key, long offset) {

        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(file, StandardOpenOption.APPEND))) {
            output.writeUTF(key.groupId());
            output.writeUTF(key.topic());
            output.writeInt(key.partition());
            output.writeLong(offset);
            output.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist consumer offset", e);
        }
    }
    private void loadOffsets() {

        try (DataInputStream input = new DataInputStream(Files.newInputStream(file))) {
            while (true) {
                try {
                    String groupId = input.readUTF();
                    String topic = input.readUTF();
                    int partition = input.readInt();
                    long offset = input.readLong();
                    OffsetKey key = new OffsetKey(groupId, topic, partition);
                    offsets.put(key, offset);
                } catch (EOFException e) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load consumer offsets", e);
        }
    }

    private void validate(String groupId, String topic, int partition, long offset) {

        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("Group ID cannot be blank");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic cannot be blank");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("Partition cannot be negative");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }
    }

    private void validateFetchRequest(String groupId, String topic, int partition) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("Group ID cannot be blank");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Topic cannot be blank");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("Partition cannot be negative");
        }
    }
    private record OffsetKey(String groupId, String topic, int partition) {
    }
}
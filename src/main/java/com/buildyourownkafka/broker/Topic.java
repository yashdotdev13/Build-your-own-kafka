package com.buildyourownkafka.broker;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Topic {

    private final String name;
    private final List<Partition> partitions;

    public Topic(String name, int partitionCount, Path dataDirectory) {
        this(name, sequentialPartitionIds(partitionCount), dataDirectory);
    }

    public Topic(String name, List<Integer> partitionIds, Path dataDirectory) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be blank");
        }
        if (partitionIds == null || partitionIds.isEmpty()) {
            throw new IllegalArgumentException("Partition IDs must not be null or empty");
        }
        if (dataDirectory == null) {
            throw new IllegalArgumentException("Data directory cannot be null");
        }
        Set<Integer> uniqueIds = new HashSet<>();
        List<Partition> partitionList = new ArrayList<>();

        for (Integer id : partitionIds) {
            if (id == null || id < 0) {
                throw new IllegalArgumentException("Partition IDs must be non-negative");
            }
            if (!uniqueIds.add(id)) {
                throw new IllegalArgumentException("Duplicate partition ID: " + id);
            }
            Path partitionDirectory = dataDirectory.resolve(name).resolve("partition-" + id);
            partitionList.add(new Partition(id, partitionDirectory));
        }
        this.name = name;
        this.partitions = Collections.unmodifiableList(partitionList);
    }

    private static List<Integer> sequentialPartitionIds(int partitionCount) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("Partition count must be greater than zero");
        }
        List<Integer> ids = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            ids.add(i);
        }
        return ids;
    }
    public String name() {
        return name;
    }
    public int partitionCount() {
        return partitions.size();
    }
    public Partition getPartition(int id) {
        return partitions.stream().filter(partition -> partition.id() == id).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Invalid partition id: " + id));
    }
    public List<Partition> partitions() {
        return partitions;
    }
}

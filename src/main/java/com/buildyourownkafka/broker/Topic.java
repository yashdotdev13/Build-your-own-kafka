package com.buildyourownkafka.broker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Topic {

    private final String name;
    private final List<Partition> partitions;

    public Topic(String name, int partitionCount) {

        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be empty");
        }

        if (partitionCount <= 0) {
            throw new IllegalArgumentException("Partition count must be greater than zero");
        }
        this.name = name;
        List<Partition> partitionList = new ArrayList<>(partitionCount);

        for (int i = 0; i < partitionCount; i++) {
            partitionList.add(new Partition(i));
        }
        this.partitions = Collections.unmodifiableList(partitionList);
    }

    public String name() {
        return name;
    }
    public int partitionCount() {
        return partitions.size();
    }
    public Partition getPartition(int id) {

        if (id < 0 || id >= partitions.size()) {
            throw new IllegalArgumentException("Invalid partition id: " + id);
        }
        return partitions.get(id);
    }
    public List<Partition> partitions() {
        return partitions;
    }
}
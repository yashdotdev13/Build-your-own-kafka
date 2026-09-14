package com.buildyourownkafka.broker;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Topic {

    private final String name;
    private final List<Partition> partitions;

    public Topic(
            String name,
            int partitionCount,
            Path dataDirectory
    ) {

        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException(
                    "Topic name cannot be blank");
        }

        if (partitionCount <= 0) {
            throw new IllegalArgumentException(
                    "Partition count must be greater than zero");
        }

        if (dataDirectory == null) {
            throw new IllegalArgumentException(
                    "Data directory cannot be null");
        }

        this.name = name;

        List<Partition> partitionList =
                new ArrayList<>();

        for (int i = 0; i < partitionCount; i++) {

            /*
             * Each partition gets its own directory.
             *
             * Example:
             *
             * data/topics/orders/partition-0
             * data/topics/orders/partition-1
             * data/topics/orders/partition-2
             */
            Path partitionDirectory =
                    dataDirectory
                            .resolve(name)
                            .resolve("partition-" + i);

            /*
             * PartitionLog manages the directory
             * and the segment files inside it.
             */
            partitionList.add(
                    new Partition(
                            i,
                            partitionDirectory
                    )
            );
        }

        this.partitions =
                Collections.unmodifiableList(
                        partitionList
                );
    }

    public String name() {
        return name;
    }

    public int partitionCount() {
        return partitions.size();
    }

    public Partition getPartition(int id) {

        if (id < 0 || id >= partitions.size()) {
            throw new IllegalArgumentException(
                    "Invalid partition id: " + id);
        }

        return partitions.get(id);
    }

    public List<Partition> partitions() {
        return partitions;
    }
}
package com.buildyourownkafka.broker;

import java.util.List;

public record JoinGroupResult(String memberId, int generation, List<Integer> partitions) {

    public JoinGroupResult {

        if (memberId == null || memberId.isBlank()) {
            throw new IllegalArgumentException("Member ID cannot be blank");
        }
        if (generation < 0) {
            throw new IllegalArgumentException("Generation cannot be negative");
        }
        if (partitions == null) {
            throw new IllegalArgumentException("Partitions cannot be null");
        }
        for (Integer partition : partitions) {
            if (partition == null || partition < 0) {
                throw new IllegalArgumentException("Partition cannot be negative");
            }
        }
        partitions = List.copyOf(partitions);
    }
}
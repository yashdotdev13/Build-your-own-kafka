package com.buildyourownkafka.broker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PartitionAssignment {

    private final Map<String, List<Integer>> assignments =
            new LinkedHashMap<>();

    public PartitionAssignment(
            List<String> members,
            int partitionCount
    ) {

        if (members == null
                || members.isEmpty()) {

            throw new IllegalArgumentException(
                    "Members cannot be empty"
            );
        }

        if (partitionCount <= 0) {

            throw new IllegalArgumentException(
                    "Partition count must be greater than zero"
            );
        }

        for (String member : members) {

            if (member == null
                    || member.isBlank()) {

                throw new IllegalArgumentException(
                        "Member ID cannot be blank"
                );
            }

            assignments.put(
                    member,
                    new ArrayList<>()
            );
        }
        for (int partition = 0;
             partition < partitionCount;
             partition++) {

            String member =
                    members.get(
                            partition % members.size()
                    );

            assignments
                    .get(member)
                    .add(partition);
        }
    }

    public List<Integer> partitionsFor(
            String memberId
    ) {

        if (memberId == null
                || memberId.isBlank()) {

            throw new IllegalArgumentException(
                    "Member ID cannot be blank"
            );
        }

        List<Integer> partitions =
                assignments.get(memberId);

        if (partitions == null) {

            return List.of();
        }

        return Collections.unmodifiableList(
                new ArrayList<>(partitions)
        );
    }

    public Map<String, List<Integer>> assignments() {

        Map<String, List<Integer>> copy =
                new LinkedHashMap<>();
        assignments.forEach(
                (member, partitions) ->
                        copy.put(
                                member,
                                List.copyOf(partitions)
                        )
        );
        return Collections.unmodifiableMap(
                copy
        );
    }

    public int memberCount() {
        return assignments.size();
    }

    public int partitionCount() {
        return assignments.values()
                .stream()
                .mapToInt(List::size)
                .sum();
    }
}
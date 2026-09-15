package com.buildyourownkafka.broker;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class PartitionAssigner {

    public PartitionAssignment assign(List<String> members, int partitionCount) {
        validate(members, partitionCount);
        List<String> sortedMembers = new ArrayList<>(members);
        sortedMembers.sort(Comparator.naturalOrder());
        return new PartitionAssignment(sortedMembers, partitionCount);
    }

    private void validate(List<String> members, int partitionCount) {
        if (members == null || members.isEmpty()) {
            throw new IllegalArgumentException("Members cannot be empty");
        }
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("Partition count must be greater than zero");
        }
        for (String member : members) {
            if (member == null || member.isBlank()) {
                throw new IllegalArgumentException("Member ID cannot be blank");
            }
        }
    }
}
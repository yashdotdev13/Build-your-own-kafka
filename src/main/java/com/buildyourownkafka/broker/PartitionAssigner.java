package com.buildyourownkafka.broker;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class PartitionAssigner {

    /*
     * ---------------------------------------------------------
     * ASSIGN PARTITIONS
     * ---------------------------------------------------------
     *
     * Uses round-robin assignment.
     *
     * Example:
     *
     * Members:
     * A, B
     *
     * Partitions:
     * 0, 1, 2, 3
     *
     * Result:
     *
     * A -> [0, 2]
     * B -> [1, 3]
     */
    public PartitionAssignment assign(
            List<String> members,
            int partitionCount
    ) {

        validate(
                members,
                partitionCount
        );

        /*
         * Sort members to make assignment deterministic.
         *
         * This is important.
         *
         * If members arrive in different orders, we still
         * want the same assignment for the same membership.
         */
        List<String> sortedMembers =
                new ArrayList<>(members);

        sortedMembers.sort(
                Comparator.naturalOrder()
        );

        return new PartitionAssignment(
                sortedMembers,
                partitionCount
        );
    }

    /*
     * ---------------------------------------------------------
     * VALIDATION
     * ---------------------------------------------------------
     */
    private void validate(
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
        }
    }
}
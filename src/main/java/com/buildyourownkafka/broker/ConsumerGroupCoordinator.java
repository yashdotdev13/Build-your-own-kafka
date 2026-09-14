package com.buildyourownkafka.broker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerGroupCoordinator {

    private final ConsumerGroupManager groupManager;

    private final Map<String, PartitionAssignment> assignments =
            new ConcurrentHashMap<>();

    public ConsumerGroupCoordinator(
            ConsumerGroupManager groupManager
    ) {

        if (groupManager == null) {

            throw new IllegalArgumentException(
                    "Consumer group manager cannot be null"
            );
        }

        this.groupManager = groupManager;
    }

    public synchronized PartitionAssignment joinGroup(
            String groupId,
            String memberId,
            int partitionCount
    ) {

        validateGroupId(groupId);
        validateMemberId(memberId);
        validatePartitionCount(partitionCount);

        groupManager.addMember(
                groupId,
                memberId
        );

        return rebalance(
                groupId,
                partitionCount
        );
    }

    public synchronized PartitionAssignment leaveGroup(
            String groupId,
            String memberId,
            int partitionCount
    ) {

        validateGroupId(groupId);
        validateMemberId(memberId);
        validatePartitionCount(partitionCount);

        groupManager.removeMember(
                groupId,
                memberId
        );

        if (!groupManager.groupExists(groupId)) {

            assignments.remove(groupId);

            return null;
        }

        return rebalance(
                groupId,
                partitionCount
        );
    }

    public synchronized PartitionAssignment getAssignment(
            String groupId
    ) {

        validateGroupId(groupId);

        return assignments.get(groupId);
    }

    private PartitionAssignment rebalance(
            String groupId,
            int partitionCount
    ) {

        PartitionAssignment assignment =
                groupManager.assignPartitions(
                        groupId,
                        partitionCount
                );

        assignments.put(
                groupId,
                assignment
        );

        return assignment;
    }

    private void validateGroupId(
            String groupId
    ) {

        if (groupId == null
                || groupId.isBlank()) {

            throw new IllegalArgumentException(
                    "Group ID cannot be blank"
            );
        }
    }

    private void validateMemberId(
            String memberId
    ) {

        if (memberId == null
                || memberId.isBlank()) {

            throw new IllegalArgumentException(
                    "Member ID cannot be blank"
            );
        }
    }

    private void validatePartitionCount(
            int partitionCount
    ) {

        if (partitionCount <= 0) {

            throw new IllegalArgumentException(
                    "Partition count must be greater than zero"
            );
        }
    }
}
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

        /*
         * Get or create the consumer group.
         */
        groupManager.getOrCreateGroup(
                groupId
        );

        /*
         * A membership change means
         * the group is preparing to rebalance.
         */
        transitionTo(
                groupId,
                ConsumerGroupState.PREPARING_REBALANCE
        );

        /*
         * Add the new member.
         */
        groupManager.addMember(
                groupId,
                memberId
        );

        /*
         * Membership is now established.
         * The coordinator is completing the rebalance.
         */
        transitionTo(
                groupId,
                ConsumerGroupState.COMPLETING_REBALANCE
        );

        /*
         * Calculate the new partition assignment.
         */
        PartitionAssignment assignment =
                rebalance(
                        groupId,
                        partitionCount
                );

        /*
         * Rebalance completed successfully.
         * Group is now stable.
         */
        transitionTo(
                groupId,
                ConsumerGroupState.STABLE
        );

        return assignment;
    }

    public synchronized PartitionAssignment leaveGroup(
            String groupId,
            String memberId,
            int partitionCount
    ) {

        validateGroupId(groupId);
        validateMemberId(memberId);
        validatePartitionCount(partitionCount);

        /*
         * The group must already exist.
         */
        if (!groupManager.groupExists(groupId)) {

            throw new ConsumerGroupException(
                    "Consumer group does not exist: "
                            + groupId
            );
        }

        /*
         * A membership change starts
         * another rebalance.
         */
        transitionTo(
                groupId,
                ConsumerGroupState.PREPARING_REBALANCE
        );

        /*
         * Remove the member.
         */
        groupManager.removeMember(
                groupId,
                memberId
        );

        /*
         * If this was the final member,
         * ConsumerGroupManager removes the group.
         */
        if (!groupManager.groupExists(groupId)) {

            assignments.remove(groupId);

            return null;
        }

        /*
         * There are still active members,
         * so complete the new rebalance.
         */
        transitionTo(
                groupId,
                ConsumerGroupState.COMPLETING_REBALANCE
        );

        /*
         * Calculate the new assignment.
         */
        PartitionAssignment assignment =
                rebalance(
                        groupId,
                        partitionCount
                );

        /*
         * Group is stable again.
         */
        transitionTo(
                groupId,
                ConsumerGroupState.STABLE
        );

        return assignment;
    }

    public synchronized PartitionAssignment getAssignment(
            String groupId
    ) {

        validateGroupId(groupId);

        return assignments.get(
                groupId
        );
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

    private void transitionTo(
            String groupId,
            ConsumerGroupState state
    ) {

        ConsumerGroup group =
                groupManager.getGroup(
                        groupId
                );

        if (group == null) {

            throw new ConsumerGroupException(
                    "Consumer group does not exist: "
                            + groupId
            );
        }

        group.setState(
                state
        );
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
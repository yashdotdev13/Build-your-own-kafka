package com.buildyourownkafka.broker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerGroupCoordinator {

    private final ConsumerGroupManager groupManager;

    private final Map<String, PartitionAssignment> assignments = new ConcurrentHashMap<>();

    private final ConsumerGroupFailureDetector failureDetector;

    /*
     * Default constructor.
     *
     * Uses a 10 second session timeout.
     */
    public ConsumerGroupCoordinator(ConsumerGroupManager groupManager) {

        this(groupManager, 10_000);
    }

    /*
     * Constructor with configurable
     * session timeout.
     */
    public ConsumerGroupCoordinator(ConsumerGroupManager groupManager, long sessionTimeoutMillis) {

        if (groupManager == null) {

            throw new IllegalArgumentException("Consumer group manager cannot be null");
        }

        this.groupManager = groupManager;

        this.failureDetector = new ConsumerGroupFailureDetector(sessionTimeoutMillis);
    }

    /*
     * Join consumer group.
     */
    public synchronized PartitionAssignment joinGroup(String groupId, String memberId, int partitionCount) {

        validateGroupId(groupId);
        validateMemberId(memberId);
        validatePartitionCount(partitionCount);

        groupManager.getOrCreateGroup(groupId);

        transitionTo(groupId, ConsumerGroupState.PREPARING_REBALANCE);

        groupManager.addMember(groupId, memberId);

        transitionTo(groupId, ConsumerGroupState.COMPLETING_REBALANCE);

        PartitionAssignment assignment = rebalance(groupId, partitionCount);

        transitionTo(groupId, ConsumerGroupState.STABLE);

        return assignment;
    }

    /*
     * Leave consumer group.
     */
    public synchronized PartitionAssignment leaveGroup(String groupId, String memberId, int partitionCount) {

        validateGroupId(groupId);
        validateMemberId(memberId);
        validatePartitionCount(partitionCount);

        if (!groupManager.groupExists(groupId)) {

            throw new ConsumerGroupException("Consumer group does not exist: " + groupId);
        }

        transitionTo(groupId, ConsumerGroupState.PREPARING_REBALANCE);

        groupManager.removeMember(groupId, memberId);

        /*
         * If the final member leaves,
         * the group manager removes the group.
         */
        if (!groupManager.groupExists(groupId)) {

            assignments.remove(groupId);

            return null;
        }

        transitionTo(groupId, ConsumerGroupState.COMPLETING_REBALANCE);

        PartitionAssignment assignment = rebalance(groupId, partitionCount);

        transitionTo(groupId, ConsumerGroupState.STABLE);

        return assignment;
    }

    /*
     * Get current partition assignment.
     */
    public synchronized PartitionAssignment getAssignment(String groupId) {

        validateGroupId(groupId);

        return assignments.get(groupId);
    }

    /*
     * Record a heartbeat from a group member.
     */
    public synchronized void heartbeat(String groupId, String memberId) {

        validateGroupId(groupId);
        validateMemberId(memberId);

        ConsumerGroup group = groupManager.getGroup(groupId);

        if (group == null) {

            throw new ConsumerGroupException("Consumer group does not exist: " + groupId);
        }

        if (!group.hasMember(memberId)) {

            throw new ConsumerGroupException("Member does not exist: " + memberId);
        }

        group.heartbeat(memberId, System.currentTimeMillis());
    }

    /*
     * Find members whose heartbeats have
     * exceeded the configured session timeout.
     *
     * This method only detects expired members.
     * It does NOT remove them.
     */
    public synchronized List<String> findExpiredMembers(String groupId, long currentTimeMillis) {

        validateGroupId(groupId);

        if (currentTimeMillis < 0) {

            throw new IllegalArgumentException("Current time cannot be negative");
        }

        ConsumerGroup group = groupManager.getGroup(groupId);

        if (group == null) {

            throw new ConsumerGroupException("Consumer group does not exist: " + groupId);
        }

        return group.members().values().stream().filter(member -> failureDetector.isExpired(member, currentTimeMillis)).map(GroupMember::memberId).sorted().toList();
    }

    /*
     * Remove expired members and
     * rebalance the group.
     */
    public synchronized PartitionAssignment removeExpiredMembers(String groupId, int partitionCount, long currentTimeMillis) {

        validateGroupId(groupId);
        validatePartitionCount(partitionCount);

        if (currentTimeMillis < 0) {

            throw new IllegalArgumentException("Current time cannot be negative");
        }

        ConsumerGroup group = groupManager.getGroup(groupId);

        if (group == null) {

            throw new ConsumerGroupException("Consumer group does not exist: " + groupId);
        }

        /*
         * Find members that have timed out.
         */
        List<String> expiredMembers = findExpiredMembers(groupId, currentTimeMillis);

        /*
         * Nothing to remove.
         *
         * Keep the current assignment unchanged.
         */
        if (expiredMembers.isEmpty()) {

            return assignments.get(groupId);
        }

        /*
         * Membership is changing.
         */
        transitionTo(groupId, ConsumerGroupState.PREPARING_REBALANCE);

        /*
         * Remove every expired member.
         */
        for (String memberId : expiredMembers) {

            groupManager.removeMember(groupId, memberId);

            /*
             * Removing the final member causes
             * ConsumerGroupManager to remove
             * the entire group.
             */
            if (!groupManager.groupExists(groupId)) {

                assignments.remove(groupId);

                return null;
            }
        }

        /*
         * Remaining members require a new assignment.
         */
        transitionTo(groupId, ConsumerGroupState.COMPLETING_REBALANCE);

        PartitionAssignment assignment = rebalance(groupId, partitionCount);

        transitionTo(groupId, ConsumerGroupState.STABLE);

        return assignment;
    }

    /*
     * Calculate and store a new partition assignment.
     */
    private PartitionAssignment rebalance(String groupId, int partitionCount) {

        PartitionAssignment assignment = groupManager.assignPartitions(groupId, partitionCount);

        assignments.put(groupId, assignment);

        return assignment;
    }

    /*
     * Change consumer group state.
     */
    private void transitionTo(String groupId, ConsumerGroupState state) {

        ConsumerGroup group = groupManager.getGroup(groupId);

        if (group == null) {

            throw new ConsumerGroupException("Consumer group does not exist: " + groupId);
        }

        group.setState(state);
    }

    /*
     * Validate group ID.
     */
    private void validateGroupId(String groupId) {

        if (groupId == null || groupId.isBlank()) {

            throw new IllegalArgumentException("Group ID cannot be blank");
        }
    }

    /*
     * Validate member ID.
     */
    private void validateMemberId(String memberId) {

        if (memberId == null || memberId.isBlank()) {

            throw new IllegalArgumentException("Member ID cannot be blank");
        }
    }

    /*
     * Validate partition count.
     */
    private void validatePartitionCount(int partitionCount) {

        if (partitionCount <= 0) {

            throw new IllegalArgumentException("Partition count must be greater than zero");
        }
    }
}
package com.buildyourownkafka.broker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerGroupCoordinator {

    private final ConsumerGroupManager groupManager;
    private final Map<String, PartitionAssignment> assignments = new ConcurrentHashMap<>();

    public ConsumerGroupCoordinator(ConsumerGroupManager groupManager) {
        if (groupManager == null) {
            throw new IllegalArgumentException("Consumer group manager cannot be null");
        }
        this.groupManager = groupManager;
    }
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
    public synchronized PartitionAssignment leaveGroup(String groupId, String memberId, int partitionCount) {

        validateGroupId(groupId);
        validateMemberId(memberId);
        validatePartitionCount(partitionCount);

        if (!groupManager.groupExists(groupId)) {
            throw new ConsumerGroupException("Consumer group does not exist: " + groupId);
        }

        transitionTo(groupId, ConsumerGroupState.PREPARING_REBALANCE);
        groupManager.removeMember(groupId, memberId);
        if (!groupManager.groupExists(groupId)) {
            assignments.remove(groupId);

            return null;
        }
        transitionTo(groupId, ConsumerGroupState.COMPLETING_REBALANCE);
        PartitionAssignment assignment = rebalance(groupId, partitionCount);
        transitionTo(groupId, ConsumerGroupState.STABLE);
        return assignment;
    }
    public synchronized PartitionAssignment getAssignment(String groupId) {
        validateGroupId(groupId);
        return assignments.get(groupId);
    }

    private PartitionAssignment rebalance(String groupId, int partitionCount) {
        PartitionAssignment assignment = groupManager.assignPartitions(groupId, partitionCount);
        assignments.put(groupId, assignment);
        return assignment;
    }

    private void transitionTo(String groupId, ConsumerGroupState state) {
        ConsumerGroup group = groupManager.getGroup(groupId);
        if (group == null) {
            throw new ConsumerGroupException("Consumer group does not exist: " + groupId);
        }
        group.setState(state);
    }
    private void validateGroupId(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("Group ID cannot be blank");
        }
    }
    private void validateMemberId(String memberId) {
        if (memberId == null || memberId.isBlank()) {
            throw new IllegalArgumentException("Member ID cannot be blank");
        }
    }
    private void validatePartitionCount(int partitionCount) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("Partition count must be greater than zero");
        }
    }
}
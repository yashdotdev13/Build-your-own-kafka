package com.buildyourownkafka.broker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerGroupCoordinator {

    private final ConsumerGroupManager groupManager;
    private final Map<String, PartitionAssignment> assignments = new ConcurrentHashMap<>();
    private final ConsumerGroupFailureDetector failureDetector;

    public ConsumerGroupCoordinator(ConsumerGroupManager groupManager) {
        this(groupManager, 10_000);
    }
    public ConsumerGroupCoordinator(ConsumerGroupManager groupManager, long sessionTimeoutMillis) {

        if (groupManager == null) {
            throw new IllegalArgumentException("Consumer group manager cannot be null");
        }
        this.groupManager = groupManager;
        this.failureDetector = new ConsumerGroupFailureDetector(sessionTimeoutMillis);
    }

    public synchronized JoinGroupResult joinGroup(String groupId, String memberId, int partitionCount) {

        validateGroupId(groupId);
        validateMemberId(memberId);
        validatePartitionCount(partitionCount);
        groupManager.getOrCreateGroup(groupId);
        transitionTo(groupId, ConsumerGroupState.PREPARING_REBALANCE);
        groupManager.addMember(groupId, memberId);
        transitionTo(groupId, ConsumerGroupState.COMPLETING_REBALANCE);
        PartitionAssignment assignment = rebalance(groupId, partitionCount);
        transitionTo(groupId, ConsumerGroupState.STABLE);
        ConsumerGroup group = groupManager.getGroup(groupId);
        GroupMember member = group.getMember(memberId);
        return new JoinGroupResult(memberId, member.generation(), assignment.partitionsFor(memberId));
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
    public synchronized PartitionAssignment removeExpiredMembers(String groupId, int partitionCount,
                                                                 long currentTimeMillis) {

        validateGroupId(groupId);
        validatePartitionCount(partitionCount);

        if (currentTimeMillis < 0) {
            throw new IllegalArgumentException("Current time cannot be negative");
        }
        ConsumerGroup group = groupManager.getGroup(groupId);
        if (group == null) {
            throw new ConsumerGroupException("Consumer group does not exist: " + groupId);
        }
        List<String> expiredMembers = findExpiredMembers(groupId, currentTimeMillis);
        if (expiredMembers.isEmpty()) {

            return assignments.get(groupId);
        }
        transitionTo(groupId, ConsumerGroupState.PREPARING_REBALANCE);
        for (String memberId : expiredMembers) {

            groupManager.removeMember(groupId, memberId);
            if (!groupManager.groupExists(groupId)) {
                assignments.remove(groupId);
                return null;
            }
        }
        transitionTo(groupId, ConsumerGroupState.COMPLETING_REBALANCE);
        PartitionAssignment assignment = rebalance(groupId, partitionCount);
        transitionTo(groupId, ConsumerGroupState.STABLE);
        return assignment;
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
package com.buildyourownkafka.broker;

import java.util.List;

public class GroupMember {

    private final String memberId;

    private final String groupId;

    private final int generation;

    private final List<Integer> assignedPartitions;

    private final long lastHeartbeat;

    public GroupMember(String memberId, String groupId, int generation, List<Integer> assignedPartitions) {
        this(memberId, groupId, generation, assignedPartitions, System.currentTimeMillis());
    }
    public GroupMember(String memberId, String groupId, int generation, List<Integer> assignedPartitions,
                       long lastHeartbeat) {
        validateMemberId(memberId);
        validateGroupId(groupId);
        if (generation < 0) {
            throw new IllegalArgumentException("Generation cannot be negative");
        }
        if (assignedPartitions == null) {
            throw new IllegalArgumentException("Assigned partitions cannot be null");
        }
        if (lastHeartbeat < 0) {
            throw new IllegalArgumentException("Last heartbeat cannot be negative");
        }
        this.memberId = memberId;
        this.groupId = groupId;
        this.generation = generation;
        this.assignedPartitions = List.copyOf(assignedPartitions);
        this.lastHeartbeat = lastHeartbeat;
    }
    public String memberId() {
        return memberId;
    }
    public String groupId() {
        return groupId;
    }
    public int generation() {
        return generation;
    }
    public List<Integer> assignedPartitions() {
        return assignedPartitions;
    }

    public long lastHeartbeat() {
        return lastHeartbeat;
    }

    public GroupMember withHeartbeat(long heartbeatTime) {
        if (heartbeatTime < 0) {
            throw new IllegalArgumentException("Heartbeat time cannot be negative");
        }
        return new GroupMember(memberId, groupId, generation, assignedPartitions, heartbeatTime);
    }

    private void validateMemberId(String memberId) {
        if (memberId == null || memberId.isBlank()) {

            throw new IllegalArgumentException("Member ID cannot be blank");
        }
    }
    private void validateGroupId(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("Group ID cannot be blank");
        }
    }
}
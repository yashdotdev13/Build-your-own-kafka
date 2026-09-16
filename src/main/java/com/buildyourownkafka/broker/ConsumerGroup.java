package com.buildyourownkafka.broker;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConsumerGroup {

    private final String groupId;
    private final Map<String, GroupMember> members = new HashMap<>();
    private ConsumerGroupState state;

    public ConsumerGroup(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("Group ID cannot be blank");
        }
        this.groupId = groupId;
        this.state = ConsumerGroupState.EMPTY;
    }

    public synchronized void addMember(GroupMember member) {
        if (member == null) {
            throw new IllegalArgumentException("Group member cannot be null");
        }
        if (!groupId.equals(member.groupId())) {
            throw new IllegalArgumentException("Member belongs to a different group");
        }
        members.put(member.memberId(), member);
    }

    public synchronized void removeMember(String memberId) {
        validateMemberId(memberId);
        members.remove(memberId);
    }

    public String groupId() {
        return groupId;
    }

    public synchronized int memberCount() {
        return members.size();
    }

    public synchronized boolean hasMember(String memberId) {
        validateMemberId(memberId);
        return members.containsKey(memberId);
    }

    public synchronized GroupMember getMember(String memberId) {
        validateMemberId(memberId);
        return members.get(memberId);
    }

    public synchronized Map<String, GroupMember> members() {
        return Collections.unmodifiableMap(new HashMap<>(members));
    }

    public synchronized ConsumerGroupState state() {
        return state;
    }

    public synchronized void setState(ConsumerGroupState state) {
        if (state == null) {
            throw new IllegalArgumentException("Group state cannot be null");
        }
        this.state = state;
    }

    public synchronized void updateMemberAssignment(String memberId, int generation, List<Integer> partitions) {
        validateMemberId(memberId);
        if (partitions == null) {
            throw new IllegalArgumentException("Partitions cannot be null");
        }
        GroupMember existingMember = members.get(memberId);
        if (existingMember == null) {
            throw new ConsumerGroupException("Member does not exist: " + memberId);
        }
        GroupMember updatedMember = new GroupMember(existingMember.memberId(), existingMember.groupId(), generation, partitions);
        members.put(memberId, updatedMember);
    }
    public synchronized void heartbeat(String memberId, long heartbeatTime) {
        validateMemberId(memberId);
        GroupMember member = members.get(memberId);
        if (member == null) {
            throw new ConsumerGroupException("Member does not exist: " + memberId);
        }
        members.put(memberId, member.withHeartbeat(heartbeatTime));
    }

    private void validateMemberId(String memberId) {
        if (memberId == null || memberId.isBlank()) {
            throw new IllegalArgumentException("Member ID cannot be blank");
        }
    }
}
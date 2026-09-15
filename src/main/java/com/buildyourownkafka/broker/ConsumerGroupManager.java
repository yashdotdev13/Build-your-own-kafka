package com.buildyourownkafka.broker;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerGroupManager {

    private final Map<String, ConsumerGroup> groups = new ConcurrentHashMap<>();

    private final PartitionAssigner partitionAssigner;

    public ConsumerGroupManager() {
        this.partitionAssigner = new PartitionAssigner();
    }
    public ConsumerGroup getOrCreateGroup(String groupId) {
        validateGroupId(groupId);
        return groups.computeIfAbsent(groupId, ConsumerGroup::new);
    }

    public ConsumerGroup getGroup(String groupId) {
        validateGroupId(groupId);
        return groups.get(groupId);
    }

    public boolean groupExists(String groupId) {
        validateGroupId(groupId);
        return groups.containsKey(groupId);
    }

    public boolean removeGroup(String groupId) {

        validateGroupId(groupId);
        ConsumerGroup group = groups.get(groupId);

        if (group == null) {
            return false;
        }

        if (group.memberCount() > 0) {
            throw new IllegalStateException("Cannot remove consumer group with active members");
        }
        return groups.remove(groupId, group);
    }
    public void addMember(String groupId, String memberId) {

        ConsumerGroup group = getOrCreateGroup(groupId);
        GroupMember member = new GroupMember(memberId, groupId, 0, List.of());
        group.addMember(member);
    }
    public void removeMember(String groupId, String memberId) {

        ConsumerGroup group = getGroup(groupId);
        if (group == null) {
            throw new IllegalArgumentException("Consumer group does not exist: " + groupId);
        }

        group.removeMember(memberId);
        if (group.memberCount() == 0) {
            groups.remove(groupId, group);
        }
    }
    public PartitionAssignment assignPartitions(String groupId, int partitionCount) {
        ConsumerGroup group = getGroup(groupId);

        if (group == null) {
            throw new IllegalArgumentException("Consumer group does not exist: " + groupId);
        }
        List<String> members = List.copyOf(group.members().keySet());
        return partitionAssigner.assign(members, partitionCount);
    }
    public Collection<ConsumerGroup> getAllGroups() {
        return Collections.unmodifiableCollection(groups.values());
    }
    public int groupCount() {
        return groups.size();
    }

    private void validateGroupId(String groupId) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("Group ID cannot be blank");
        }
    }
}
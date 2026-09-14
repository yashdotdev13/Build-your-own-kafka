package com.buildyourownkafka.broker;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerGroupManager {

    private final Map<String, ConsumerGroup> groups =
            new ConcurrentHashMap<>();

    private final PartitionAssigner partitionAssigner;

    public ConsumerGroupManager() {

        this.partitionAssigner =
                new PartitionAssigner();
    }

    /*
     * ---------------------------------------------------------
     * GET OR CREATE GROUP
     * ---------------------------------------------------------
     */
    public ConsumerGroup getOrCreateGroup(
            String groupId
    ) {

        validateGroupId(groupId);

        return groups.computeIfAbsent(
                groupId,
                ConsumerGroup::new
        );
    }

    /*
     * ---------------------------------------------------------
     * GET GROUP
     * ---------------------------------------------------------
     */
    public ConsumerGroup getGroup(
            String groupId
    ) {

        validateGroupId(groupId);

        return groups.get(groupId);
    }

    /*
     * ---------------------------------------------------------
     * GROUP EXISTS
     * ---------------------------------------------------------
     */
    public boolean groupExists(
            String groupId
    ) {

        validateGroupId(groupId);

        return groups.containsKey(groupId);
    }

    /*
     * ---------------------------------------------------------
     * REMOVE GROUP
     * ---------------------------------------------------------
     *
     * A group can only be removed when it has no members.
     */
    public boolean removeGroup(
            String groupId
    ) {

        validateGroupId(groupId);

        ConsumerGroup group =
                groups.get(groupId);

        if (group == null) {

            return false;
        }

        if (group.memberCount() > 0) {

            throw new IllegalStateException(
                    "Cannot remove consumer group with active members"
            );
        }

        return groups.remove(
                groupId,
                group
        );
    }

    /*
     * ---------------------------------------------------------
     * ADD MEMBER
     * ---------------------------------------------------------
     */
    public void addMember(
            String groupId,
            String memberId
    ) {

        ConsumerGroup group =
                getOrCreateGroup(groupId);

        group.addMember(
                memberId
        );
    }

    /*
     * ---------------------------------------------------------
     * REMOVE MEMBER
     * ---------------------------------------------------------
     *
     * If the last member leaves, the group is removed.
     */
    public void removeMember(
            String groupId,
            String memberId
    ) {

        ConsumerGroup group =
                getGroup(groupId);

        if (group == null) {

            throw new IllegalArgumentException(
                    "Consumer group does not exist: "
                            + groupId
            );
        }

        group.removeMember(
                memberId
        );

        /*
         * Remove empty group.
         */
        if (group.memberCount() == 0) {

            groups.remove(
                    groupId,
                    group
            );
        }
    }

    /*
     * ---------------------------------------------------------
     * ASSIGN PARTITIONS
     * ---------------------------------------------------------
     *
     * Calculates partition assignment for a group.
     *
     * Example:
     *
     * Members:
     *
     * consumer-A
     * consumer-B
     *
     * Partitions:
     *
     * 0, 1, 2, 3
     *
     * Result:
     *
     * consumer-A -> [0, 2]
     * consumer-B -> [1, 3]
     */
    public PartitionAssignment assignPartitions(
            String groupId,
            int partitionCount
    ) {

        ConsumerGroup group =
                getGroup(groupId);

        if (group == null) {

            throw new IllegalArgumentException(
                    "Consumer group does not exist: "
                            + groupId
            );
        }

        /*
         * Take a snapshot of the current members.
         *
         * ConsumerGroup protects its internal state,
         * so we don't expose the original Set.
         */
        List<String> members =
                List.copyOf(
                        group.members()
                );

        return partitionAssigner.assign(
                members,
                partitionCount
        );
    }

    /*
     * ---------------------------------------------------------
     * GET ALL GROUPS
     * ---------------------------------------------------------
     */
    public Collection<ConsumerGroup> getAllGroups() {

        return Collections.unmodifiableCollection(
                groups.values()
        );
    }
    public int groupCount() {

        return groups.size();
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
}
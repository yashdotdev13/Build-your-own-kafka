package com.buildyourownkafka.broker;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ConsumerGroup {

    private final String groupId;

    private final Set<String> members =
            new HashSet<>();

    public ConsumerGroup(String groupId) {

        if (groupId == null
                || groupId.isBlank()) {

            throw new IllegalArgumentException(
                    "Group ID cannot be blank"
            );
        }

        this.groupId = groupId;
    }

    public synchronized void addMember(
            String memberId
    ) {

        validateMemberId(memberId);

        members.add(memberId);
    }

    public synchronized void removeMember(
            String memberId
    ) {

        validateMemberId(memberId);

        members.remove(memberId);
    }

    public String groupId() {
        return groupId;
    }

    public synchronized int memberCount() {
        return members.size();
    }

    public synchronized boolean hasMember(
            String memberId
    ) {

        validateMemberId(memberId);

        return members.contains(memberId);
    }

    public synchronized Set<String> members() {
        return Collections.unmodifiableSet(
                new HashSet<>(members)
        );
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
}
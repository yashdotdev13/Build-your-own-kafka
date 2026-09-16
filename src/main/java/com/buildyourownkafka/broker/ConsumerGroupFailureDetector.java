package com.buildyourownkafka.broker;

public class ConsumerGroupFailureDetector {

    private final long sessionTimeoutMillis;
    public ConsumerGroupFailureDetector(long sessionTimeoutMillis) {

        if (sessionTimeoutMillis <= 0) {
            throw new IllegalArgumentException("Session timeout must be greater than zero");
        }
        this.sessionTimeoutMillis = sessionTimeoutMillis;
    }
    public long sessionTimeoutMillis() {
        return sessionTimeoutMillis;
    }
    public boolean isExpired(GroupMember member, long currentTimeMillis) {

        if (member == null) {
            throw new IllegalArgumentException("Group member cannot be null");
        }
        if (currentTimeMillis < 0) {
            throw new IllegalArgumentException("Current time cannot be negative");
        }
        long lastHeartbeat = member.lastHeartbeat();
        return currentTimeMillis - lastHeartbeat > sessionTimeoutMillis;
    }
}
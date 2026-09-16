package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroup;
import com.buildyourownkafka.broker.ConsumerGroupCoordinator;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.GroupMember;

import java.util.List;

public class ConsumerGroupHeartbeatTest {

    public static void main(String[] args) {

        System.out.println("=== CONSUMER GROUP HEARTBEAT TEST ===");

        ConsumerGroupManager groupManager = new ConsumerGroupManager();
        ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator(groupManager);
        ConsumerGroup group = groupManager.getOrCreateGroup("orders-group");
        GroupMember member = new GroupMember("consumer-A", "orders-group", 1, List.of(0, 2));

        group.addMember(member);
        GroupMember before = group.getMember("consumer-A");

        long initialHeartbeat = before.lastHeartbeat();
        System.out.println("Initial heartbeat: " + initialHeartbeat);
        coordinator.heartbeat("orders-group", "consumer-A");
        GroupMember after = group.getMember("consumer-A");

        long updatedHeartbeat = after.lastHeartbeat();
        System.out.println("Updated heartbeat: " + updatedHeartbeat);
        if (updatedHeartbeat < initialHeartbeat) {
            throw new RuntimeException("Heartbeat timestamp should not move backwards");
        }
        if (after.generation() != 1) {
            throw new RuntimeException("Heartbeat should not change generation");
        }
        if (!after.assignedPartitions().equals(List.of(0, 2))) {
            throw new RuntimeException("Heartbeat should not change partitions");
        }
        System.out.println();
        System.out.println("Coordinator heartbeat verified successfully!");
    }
}
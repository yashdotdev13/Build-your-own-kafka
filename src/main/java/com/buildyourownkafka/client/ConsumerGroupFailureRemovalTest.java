package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroup;
import com.buildyourownkafka.broker.ConsumerGroupCoordinator;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.ConsumerGroupState;
import com.buildyourownkafka.broker.GroupMember;
import com.buildyourownkafka.broker.PartitionAssignment;

import java.util.List;

public class ConsumerGroupFailureRemovalTest {

    public static void main(String[] args) {

        System.out.println("=== CONSUMER GROUP FAILURE REMOVAL TEST ===");
        ConsumerGroupManager manager = new ConsumerGroupManager();
        ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator(manager, 10_000);
        ConsumerGroup group = manager.getOrCreateGroup("orders-group");
        group.addMember(new GroupMember("consumer-A", "orders-group", 1, List.of(0, 2), 100_000));
        group.addMember(new GroupMember("consumer-B", "orders-group", 1, List.of(1, 3), 90_000));

        PartitionAssignment assignment = coordinator.removeExpiredMembers("orders-group", 4, 105_000);
        System.out.println("New assignment: " + assignment.assignments());

        if (groupManagerDoesNotExist(manager, "orders-group")) {
            throw new RuntimeException("Group should still exist");
        }

        group = manager.getGroup("orders-group");
        if (group.hasMember("consumer-B")) {
            throw new RuntimeException("Expired member should be removed");
        }
        if (!group.hasMember("consumer-A")) {
            throw new RuntimeException("Alive member should remain");
        }
        if (!assignment.partitionsFor("consumer-A").equals(List.of(0, 1, 2, 3))) {
            throw new RuntimeException("Remaining member should own all partitions");
        }
        if (group.state() != ConsumerGroupState.STABLE) {
            throw new RuntimeException("Group should be STABLE after rebalance");
        }
        PartitionAssignment storedAssignment = coordinator.getAssignment("orders-group");
        if (storedAssignment == null) {
            throw new RuntimeException("Assignment should be stored");
        }
        if (!storedAssignment.partitionsFor("consumer-A").equals(List.of(0, 1, 2, 3))) {
            throw new RuntimeException("Stored assignment is incorrect");
        }
        System.out.println("Remaining members: " + group.members().keySet());
        System.out.println("Group state: " + group.state());
        System.out.println();
        System.out.println("Expired member removal and " + "rebalance verified successfully!");
    }

    private static boolean groupManagerDoesNotExist(ConsumerGroupManager manager, String groupId) {
        return !manager.groupExists(groupId);
    }
}
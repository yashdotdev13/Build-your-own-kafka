package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroup;
import com.buildyourownkafka.broker.ConsumerGroupCoordinator;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.ConsumerGroupState;
import com.buildyourownkafka.broker.JoinGroupResult;
import com.buildyourownkafka.broker.PartitionAssignment;

import java.util.List;

public class ConsumerGroupCoordinatorTest {

    public static void main(String[] args) {

        System.out.println("=== CONSUMER GROUP COORDINATOR TEST ===");
        ConsumerGroupManager manager = new ConsumerGroupManager();
        ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator(manager);
        JoinGroupResult resultA = coordinator.joinGroup("orders-group", "consumer-A", 4);
        System.out.println("Consumer-A initial partitions: " + resultA.partitions());
        if (!resultA.memberId().equals("consumer-A")) {
            throw new RuntimeException("Incorrect Consumer-A member ID");
        }

        if (!resultA.partitions().equals(List.of(0, 1, 2, 3))) {
            throw new RuntimeException("Consumer-A should initially own all partitions");
        }
        JoinGroupResult resultB = coordinator.joinGroup("orders-group", "consumer-B", 4);

        System.out.println("Consumer-B partitions: " + resultB.partitions());
        if (!resultB.memberId().equals("consumer-B")) {
            throw new RuntimeException("Incorrect Consumer-B member ID");
        }
        PartitionAssignment currentAssignment = coordinator.getAssignment("orders-group");
        if (currentAssignment == null) {
            throw new RuntimeException("Current assignment should exist");
        }
        List<Integer> currentPartitionsA = currentAssignment.partitionsFor("consumer-A");
        List<Integer> currentPartitionsB = currentAssignment.partitionsFor("consumer-B");
        System.out.println("Consumer-A current partitions: " + currentPartitionsA);
        System.out.println("Consumer-B current partitions: " + currentPartitionsB);
        if (!currentPartitionsA.equals(List.of(0, 2))) {

            throw new RuntimeException("Incorrect current assignment for Consumer-A");
        }

        if (!currentPartitionsB.equals(List.of(1, 3))) {
            throw new RuntimeException("Incorrect current assignment for Consumer-B");
        }
        int totalAssigned = currentPartitionsA.size() + currentPartitionsB.size();

        if (totalAssigned != 4) {
            throw new RuntimeException("All 4 partitions should be assigned");
        }
        for (Integer partition : currentPartitionsA) {
            if (currentPartitionsB.contains(partition)) {
                throw new RuntimeException("Partition assigned to multiple members: " + partition);
            }
        }
        ConsumerGroup group = manager.getGroup("orders-group");

        if (group == null) {
            throw new RuntimeException("Consumer group should exist");
        }
        if (group.memberCount() != 2) {
            throw new RuntimeException("Expected 2 group members");
        }
        if (!group.hasMember("consumer-A")) {
            throw new RuntimeException("Consumer-A should exist");
        }

        if (!group.hasMember("consumer-B")) {
            throw new RuntimeException("Consumer-B should exist");
        }
        if (group.state() != ConsumerGroupState.STABLE) {
            throw new RuntimeException("Group should be STABLE");
        }
        System.out.println("Consumer-A generation: " + resultA.generation());
        System.out.println("Consumer-B generation: " + resultB.generation());
        System.out.println("Group state: " + group.state());
        System.out.println();
        System.out.println("Consumer group coordinator " + "verified successfully!");
    }
}
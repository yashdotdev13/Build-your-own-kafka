package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroup;
import com.buildyourownkafka.broker.ConsumerGroupCoordinator;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.GroupMember;
import com.buildyourownkafka.broker.JoinGroupResult;
import com.buildyourownkafka.broker.PartitionAssignment;

public class ConsumerGroupFailureRebalanceTest {

    public static void main(String[] args) {

        System.out.println("=== CONSUMER GROUP FAILURE REBALANCE TEST ===");

        String groupId = "failure-rebalance-group";
        int partitionCount = 4;

        ConsumerGroupManager manager = new ConsumerGroupManager();
        ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator(manager, 5000);

        System.out.println();
        System.out.println("=== JOIN GROUP ===");

        JoinGroupResult resultA = coordinator.joinGroup(groupId, "consumer-A", partitionCount);
        JoinGroupResult resultB = coordinator.joinGroup(groupId, "consumer-B", partitionCount);
        JoinGroupResult resultC = coordinator.joinGroup(groupId, "consumer-C", partitionCount);

        ConsumerGroup group = manager.getGroup(groupId);

        System.out.println("Members: " + group.memberCount());
        System.out.println("Current generation: " + group.generation());

        System.out.println();
        System.out.println("=== INITIAL ASSIGNMENT ===");

        PartitionAssignment initialAssignment = coordinator.getAssignment(groupId);
        System.out.println("Consumer A: " + initialAssignment.partitionsFor("consumer-A"));
        System.out.println("Consumer B: " + initialAssignment.partitionsFor("consumer-B"));
        System.out.println("Consumer C: " + initialAssignment.partitionsFor("consumer-C"));
        long now = System.currentTimeMillis();

        GroupMember expiredConsumerA = new GroupMember("consumer-A", groupId, group.generation(), initialAssignment.partitionsFor("consumer-A"), now - 10_000);

        group.addMember(expiredConsumerA);
        coordinator.heartbeat(groupId, "consumer-B");

        coordinator.heartbeat(groupId, "consumer-C");
        long checkTime = now + 4_000;

        System.out.println();
        System.out.println("=== FAILURE DETECTION ===");

        System.out.println("Expired members: " + coordinator.findExpiredMembers(groupId, checkTime));
        PartitionAssignment newAssignment = coordinator.removeExpiredMembers(groupId, partitionCount, checkTime);

        System.out.println();
        System.out.println("=== AFTER FAILURE REBALANCE ===");

        System.out.println("Members remaining: " + group.memberCount());
        System.out.println("New generation: " + group.generation());

        if (newAssignment == null) {
            throw new RuntimeException("New assignment should not be null");
        }
        System.out.println("Consumer B: " + newAssignment.partitionsFor("consumer-B"));
        System.out.println("Consumer C: " + newAssignment.partitionsFor("consumer-C"));
        if (group.hasMember("consumer-A")) {
            throw new RuntimeException("Consumer A should have been removed");
        }
        if (!group.hasMember("consumer-B") || !group.hasMember("consumer-C")) {
            throw new RuntimeException("Consumer B and C should remain");
        }
        if (newAssignment.partitionCount() != partitionCount) {
            throw new RuntimeException("All partitions should be reassigned");
        }
        if (group.generation() <= resultC.generation()) {
            throw new RuntimeException("Generation should increase after failure rebalance");
        }
        System.out.println();
        System.out.println("CONSUMER FAILURE REBALANCE VERIFIED!");
    }
}
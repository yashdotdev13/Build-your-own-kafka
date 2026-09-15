package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroup;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.GroupMember;
import com.buildyourownkafka.broker.PartitionAssignment;

import java.util.List;

public class ConsumerGroupManagerTest {

    public static void main(String[] args) {

        System.out.println("=== CONSUMER GROUP MANAGER TEST ===");
        ConsumerGroupManager manager = new ConsumerGroupManager();
        ConsumerGroup paymentGroup = manager.getOrCreateGroup("payment-service");

        GroupMember consumer1 = new GroupMember("consumer-1", "payment-service", 0, List.of());
        GroupMember consumer2 = new GroupMember("consumer-2", "payment-service", 0, List.of());
        paymentGroup.addMember(consumer1);
        paymentGroup.addMember(consumer2);

        PartitionAssignment assignment = manager.assignPartitions("payment-service", 4);
        System.out.println("Payment assignment: " + assignment.assignments());

        if (!assignment.partitionsFor("consumer-1").equals(List.of(0, 2))) {
            throw new RuntimeException("Incorrect assignment for consumer-1");
        }
        if (!assignment.partitionsFor("consumer-2").equals(List.of(1, 3))) {
            throw new RuntimeException("Incorrect assignment for consumer-2");
        }

        ConsumerGroup analyticsGroup = manager.getOrCreateGroup("analytics-service");
        GroupMember consumer3 = new GroupMember("consumer-3", "analytics-service", 0, List.of());
        analyticsGroup.addMember(consumer3);
        System.out.println("Group count: " + manager.groupCount());
        System.out.println("Payment members: " + paymentGroup.members());
        System.out.println("Analytics members: " + analyticsGroup.members());
        if (!manager.groupExists("payment-service")) {
            throw new RuntimeException("Payment group should exist");
        }

        if (!manager.groupExists("analytics-service")) {
            throw new RuntimeException("Analytics group should exist");
        }
        if (manager.groupCount() != 2) {
            throw new RuntimeException("Expected 2 consumer groups");
        }
        manager.removeMember("payment-service", "consumer-1");
        if (paymentGroup.memberCount() != 1) {
            throw new RuntimeException("Expected one remaining member");
        }
        manager.removeMember("payment-service", "consumer-2");
        if (manager.groupExists("payment-service")) {
            throw new RuntimeException("Empty consumer group should be removed");
        }
        if (!manager.groupExists("analytics-service")) {
            throw new RuntimeException("Analytics group should still exist");
        }
        System.out.println();
        System.out.println("ConsumerGroupManager verified successfully!");
    }
}
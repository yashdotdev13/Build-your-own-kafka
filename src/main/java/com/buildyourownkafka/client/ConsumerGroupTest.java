package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroup;
import com.buildyourownkafka.broker.ConsumerGroupState;
import com.buildyourownkafka.broker.GroupMember;

import java.util.List;

public class ConsumerGroupTest {

    public static void main(String[] args) {

        System.out.println("=== CONSUMER GROUP TEST ===");
        ConsumerGroup group = new ConsumerGroup("payment-service");
        GroupMember memberA = new GroupMember("consumer-1", "payment-service", 1, List.of(0, 2));
        GroupMember memberB = new GroupMember("consumer-2", "payment-service", 1, List.of(1, 3));
        group.addMember(memberA);
        group.addMember(memberB);
        System.out.println("Group ID: " + group.groupId());
        System.out.println("Member count: " + group.memberCount());
        System.out.println("Members: " + group.members().keySet());
        GroupMember retrieved = group.getMember("consumer-1");
        System.out.println("Consumer-1 generation: " + retrieved.generation());
        System.out.println("Consumer-1 partitions: " + retrieved.assignedPartitions());

        if (!group.hasMember("consumer-1")) {
            throw new RuntimeException("consumer-1 should exist");
        }

        if (group.memberCount() != 2) {
            throw new RuntimeException("Expected 2 members");
        }

        if (retrieved.generation() != 1) {
            throw new RuntimeException("Incorrect generation");
        }

        if (!retrieved.assignedPartitions().equals(List.of(0, 2))) {
            throw new RuntimeException("Incorrect partition assignment");
        }
        group.updateMemberAssignment("consumer-1", 2, List.of(1, 3));
        retrieved = group.getMember("consumer-1");

        if (retrieved.generation() != 2) {
            throw new RuntimeException("Generation should be updated");
        }
        if (!retrieved.assignedPartitions().equals(List.of(1, 3))) {
            throw new RuntimeException("Partitions should be updated");
        }
        group.setState(ConsumerGroupState.PREPARING_REBALANCE);
        System.out.println("State changed to: " + group.state());
        group.setState(ConsumerGroupState.COMPLETING_REBALANCE);
        System.out.println("State changed to: " + group.state());
        group.setState(ConsumerGroupState.STABLE);
        System.out.println("State changed to: " + group.state());
        group.removeMember("consumer-2");
        if (group.memberCount() != 1) {
            throw new RuntimeException("Expected 1 member after removal");
        }
        if (group.hasMember("consumer-2")) {
            throw new RuntimeException("consumer-2 should be removed");
        }
        System.out.println();
        System.out.println("ConsumerGroup member metadata " + "lifecycle verified successfully!");
    }
}
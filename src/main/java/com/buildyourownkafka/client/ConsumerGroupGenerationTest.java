package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroup;
import com.buildyourownkafka.broker.GroupMember;

public class ConsumerGroupGenerationTest {

    public static void main(String[] args) {

        System.out.println("=== CONSUMER GROUP GENERATION TEST ===");
        ConsumerGroup group = new ConsumerGroup("orders-group");
        System.out.println("Initial generation: " + group.generation());

        if (group.generation() != 0) {
            throw new AssertionError("Initial generation must be zero");
        }
        int generation1 = group.incrementGeneration();
        System.out.println("Generation after first increment: " + generation1);
        if (generation1 != 1) {
            throw new AssertionError("Expected generation 1");
        }
        int generation2 = group.incrementGeneration();
        System.out.println("Generation after second increment: " + generation2);
        if (generation2 != 2) {
            throw new AssertionError("Expected generation 2");
        }
        if (group.generation() != 2) {
            throw new AssertionError("Group generation was not updated");
        }
        GroupMember member = new GroupMember("consumer-A", "orders-group", group.generation(), java.util.List.of());
        group.addMember(member);
        if (!group.hasMember("consumer-A")) {
            throw new AssertionError("Member was not added");
        }

        System.out.println("Member generation: " + group.getMember("consumer-A").generation());
        System.out.println();
        System.out.println("Consumer group generation " + "verified successfully!");
    }
}
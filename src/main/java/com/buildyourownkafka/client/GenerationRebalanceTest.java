package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroup;
import com.buildyourownkafka.broker.ConsumerGroupCoordinator;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.JoinGroupResult;

public class GenerationRebalanceTest {

    public static void main(String[] args) {

        System.out.println("=== GENERATION REBALANCE TEST ===");
        ConsumerGroupManager groupManager = new ConsumerGroupManager();

        ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator(groupManager);
        JoinGroupResult resultA = coordinator.joinGroup("orders-group", "consumer-A", 4);

        ConsumerGroup group = groupManager.getGroup("orders-group");

        System.out.println("Generation after consumer-A joins: " + group.generation());
        System.out.println("Consumer-A generation: " + group.getMember("consumer-A").generation());

        if (group.generation() != 1) {
            throw new AssertionError("Expected generation 1");
        }
        if (group.getMember("consumer-A").generation() != 1) {
            throw new AssertionError("Consumer-A should be on generation 1");
        }
        JoinGroupResult resultB = coordinator.joinGroup("orders-group", "consumer-B", 4);

        System.out.println("Generation after consumer-B joins: " + group.generation());
        System.out.println("Consumer-A generation: " + group.getMember("consumer-A").generation());
        System.out.println("Consumer-B generation: " + group.getMember("consumer-B").generation());
        if (group.generation() != 2) {
            throw new AssertionError("Expected generation 2");
        }
        if (group.getMember("consumer-A").generation() != 2) {
            throw new AssertionError("Consumer-A should be on generation 2");
        }
        if (group.getMember("consumer-B").generation() != 2) {
            throw new AssertionError("Consumer-B should be on generation 2");
        }
        if (group.memberCount() != 2) {
            throw new AssertionError("Expected two members");
        }
        System.out.println();
        System.out.println("Generation-aware rebalance " + "verified successfully!");
    }
}
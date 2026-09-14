package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroup;
import com.buildyourownkafka.broker.ConsumerGroupManager;

public class ConsumerGroupManagerTest {

    public static void main(String[] args) {

        System.out.println("=== CONSUMER GROUP MANAGER TEST ===");

        ConsumerGroupManager manager = new ConsumerGroupManager();
        ConsumerGroup paymentGroup = manager.getOrCreateGroup("payment-service");
        paymentGroup.addMember("consumer-1");
        paymentGroup.addMember("consumer-2");
        ConsumerGroup analyticsGroup = manager.getOrCreateGroup("analytics-service");

        analyticsGroup.addMember("consumer-3");
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
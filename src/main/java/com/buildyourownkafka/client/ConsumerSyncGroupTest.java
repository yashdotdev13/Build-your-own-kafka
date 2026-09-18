package com.buildyourownkafka.client;

import java.util.List;

public class ConsumerSyncGroupTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== CONSUMER SYNC GROUP TEST ===");

        try (Consumer consumer = new Consumer("localhost", 9092, "orders", 0, "orders-group")) {
            System.out.println("Initial member ID: " + consumer.memberId());
            System.out.println("Initial generation: " + consumer.generation());
            if (consumer.memberId() == null) {
                throw new AssertionError("Member ID should not be null");
            }
            if (consumer.generation() != 0) {
                throw new AssertionError("Initial generation should be 0");
            }
            consumer.joinGroup(4);

            System.out.println();
            System.out.println("After JOIN_GROUP:");
            System.out.println("Member ID: " + consumer.memberId());
            System.out.println("Generation: " + consumer.generation());

            if (!consumer.isGroupMember()) {
                throw new AssertionError("Consumer should be a group member");
            }
            if (consumer.generation() <= 0) {
                throw new AssertionError("Generation should be greater than zero");
            }
            int joinedGeneration = consumer.generation();
            consumer.syncGroup();

            System.out.println();
            System.out.println("After SYNC_GROUP:");
            System.out.println("Generation: " + consumer.generation());
            List<Integer> assignments = consumer.assignedPartitions();
            System.out.println("Assigned partitions: " + assignments);
            if (consumer.generation() != joinedGeneration) {

                throw new AssertionError("SYNC_GROUP changed the generation unexpectedly");
            }
            if (assignments == null) {
                throw new AssertionError("Assignment cannot be null");
            }
            if (assignments.isEmpty()) {
                throw new AssertionError("Consumer should receive at least one partition");
            }
            for (Integer partition : assignments) {
                if (partition == null || partition < 0) {
                    throw new AssertionError("Invalid partition assignment: " + partition);
                }
            }
            System.out.println();
            System.out.println("CONSUMER SYNC GROUP VERIFIED!");
        }
    }
}
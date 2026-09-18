package com.buildyourownkafka.client;

public class ConsumerJoinGroupTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== CONSUMER JOIN GROUP TEST ===");
        try (Consumer consumer = new Consumer("localhost", 9092, "orders", 0, "orders-group")) {
            System.out.println("Consumer created.");
            System.out.println("Initial member ID: " + consumer.memberId());
            System.out.println("Initial generation: " + consumer.generation());
            if (consumer.memberId() == null) {
                throw new AssertionError("Member ID should be initialized");
            }
            if (consumer.generation() != 0) {
                throw new AssertionError("Initial generation should be 0");
            }
            // Join consumer group
            consumer.joinGroup(4);
            System.out.println("Consumer joined group.");
            System.out.println("Member ID: " + consumer.memberId());
            System.out.println("Generation: " + consumer.generation());

            if (!consumer.isGroupMember()) {
                throw new AssertionError("Consumer should be a group member");
            }
            if (consumer.generation() <= 0) {
                throw new AssertionError("Generation should be greater than zero");
            }
            System.out.println();
            System.out.println("CONSUMER JOIN GROUP VERIFIED!");
        }
    }
}
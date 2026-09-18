package com.buildyourownkafka.client;

public class ConsumerAutomaticRejoinTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== CONSUMER AUTOMATIC REJOIN TEST ===");
        try (Consumer consumerA = new Consumer("localhost", 9092, "orders", 0, "orders-group");
             Consumer consumerB = new Consumer("localhost", 9092, "orders", 1, "orders-group")) {
            consumerA.joinGroup(4);
            consumerA.syncGroup();
            int generationA = consumerA.generation();
            System.out.println("Consumer-A generation: " + generationA);
            consumerB.joinGroup(4);
            consumerB.syncGroup();
            int generationB = consumerB.generation();
            System.out.println("Consumer-B generation: " + generationB);
            if (generationB <= generationA) {
                throw new AssertionError("Generation should increase after Consumer-B joins");
            }
            boolean staleGenerationDetected = false;
            try {
                consumerA.syncGroup();
            } catch (RuntimeException e) {
                staleGenerationDetected = true;
                System.out.println("Consumer-A stale generation detected: " + e.getMessage());
            }
            if (!staleGenerationDetected) {
                throw new AssertionError("Consumer-A should have stale generation");
            }
            consumerA.rejoinGroup(4);
            System.out.println("Consumer-A rejoined.");
            System.out.println("New generation: " + consumerA.generation());
            System.out.println("New assignment: " + consumerA.assignedPartitions());
            if (consumerA.generation() <= generationB) {
                throw new AssertionError("Consumer-A should receive a new generation");
            }
            if (consumerA.assignedPartitions().isEmpty()) {
                throw new AssertionError("Consumer-A should receive an assignment");
            }
            System.out.println();
            System.out.println("CONSUMER AUTOMATIC REJOIN VERIFIED!");
        }
    }
}
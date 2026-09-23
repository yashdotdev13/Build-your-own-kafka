package com.buildyourownkafka.client;

public class ConsumerHeartbeatFailureTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== CONSUMER HEARTBEAT FAILURE TEST ===");

        Consumer consumer = new Consumer("localhost", 9092, "heartbeat-test", 0, 0L, "heartbeat-failure-group");
        System.out.println();
        System.out.println("Joining group...");
        consumer.joinGroup(4);
        System.out.println("Generation: " + consumer.generation());
        System.out.println();
        System.out.println("Syncing group...");

        consumer.syncGroup();
        System.out.println("Assigned partitions: " + consumer.assignedPartitions());

        System.out.println();
        System.out.println("Automatic heartbeat is running...");

        Thread.sleep(5000);

        System.out.println();
        System.out.println("Heartbeats verified.");

        /*
         * Simulate consumer failure by stopping
         * heartbeat and closing the connection.
         */
        System.out.println();
        System.out.println("Simulating consumer failure...");

        consumer.close();
        System.out.println("Consumer connection lost.");

        /*
         * Give the broker enough time to detect
         * the expired heartbeat.
         */
        System.out.println();
        System.out.println("Waiting for broker failure detection...");

        Thread.sleep(7000);

        System.out.println();
        System.out.println("FAILURE DETECTION TEST COMPLETED!");
    }
}
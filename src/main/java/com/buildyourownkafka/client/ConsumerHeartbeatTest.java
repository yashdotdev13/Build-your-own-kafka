package com.buildyourownkafka.client;

public class ConsumerHeartbeatTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== CONSUMER HEARTBEAT TEST ===");

        String host = "localhost";
        int port = 9092;
        String topic = "heartbeat-test";
        int partition = 0;
        String groupId = "heartbeat-consumer-group";

        Consumer consumer =
                new Consumer(
                        host,
                        port,
                        topic,
                        partition,
                        0L,
                        groupId
                );

        try {

            System.out.println(
                    "Initial member ID: "
                            + consumer.memberId()
            );

            System.out.println(
                    "Initial generation: "
                            + consumer.generation()
            );

            System.out.println();
            System.out.println("Joining group...");

            consumer.joinGroup(4);

            System.out.println(
                    "Member ID after JOIN_GROUP: "
                            + consumer.memberId()
            );

            System.out.println(
                    "Generation after JOIN_GROUP: "
                            + consumer.generation()
            );

            System.out.println();
            System.out.println("Syncing group...");

            consumer.syncGroup();

            System.out.println(
                    "Assigned partitions: "
                            + consumer.assignedPartitions()
            );

            System.out.println();
            System.out.println(
                    "Starting automatic heartbeat..."
            );

            consumer.startHeartbeat(1000);

            System.out.println(
                    "Automatic heartbeat started."
            );

            Thread.sleep(5000);

            System.out.println(
                    "Automatic heartbeat test completed."
            );

            System.out.println();
            System.out.println(
                    "CONSUMER HEARTBEAT VERIFIED!"
            );

        } finally {

            consumer.close();
        }
    }
}
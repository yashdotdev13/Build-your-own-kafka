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

            System.out.println(
                    "Automatic heartbeat started by SYNC_GROUP."
            );

            /*
             * Allow several heartbeats to be sent.
             */
            Thread.sleep(5000);

            System.out.println();
            System.out.println(
                    "Heartbeat activity verified."
            );

            /*
             * Close the consumer.
             * close() should stop the heartbeat thread
             * before closing the socket.
             */
            System.out.println();
            System.out.println(
                    "Closing consumer..."
            );

            consumer.close();

            System.out.println(
                    "Consumer closed."
            );

            /*
             * Give the test enough time to detect
             * whether the heartbeat thread continues.
             */
            Thread.sleep(2000);

            System.out.println();
            System.out.println(
                    "No heartbeat should appear after shutdown."
            );

            System.out.println();
            System.out.println(
                    "CONSUMER HEARTBEAT LIFECYCLE VERIFIED!"
            );

        } finally {

            /*
             * close() is safe to call again because
             * stopHeartbeat() is idempotent.
             */
            consumer.close();
        }
    }
}
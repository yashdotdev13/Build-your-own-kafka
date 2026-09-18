package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroupCoordinator;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.JoinGroupResult;

public class ConsumerGroupRejoinTest {

    public static void main(String[] args) {

        System.out.println("=== CONSUMER GROUP REJOIN TEST ===");
        ConsumerGroupManager groupManager = new ConsumerGroupManager();
        ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator(groupManager);
        String groupId = "orders-group";
        int partitionCount = 4;
        JoinGroupResult firstJoin = coordinator.joinGroup(groupId, "consumer-A", partitionCount);
        System.out.println("Consumer-A first generation: " + firstJoin.generation());
        if (firstJoin.generation() != 1) {
            throw new AssertionError("Expected first generation to be 1");
        }
        JoinGroupResult secondJoin = coordinator.joinGroup(groupId, "consumer-B", partitionCount);
        System.out.println("Consumer-B generation: " + secondJoin.generation());
        if (secondJoin.generation() != 2) {
            throw new AssertionError("Expected second generation to be 2");
        }
        boolean staleGenerationRejected = false;
        try {
            coordinator.syncGroup(groupId, "consumer-A", firstJoin.generation());
        } catch (Exception e) {
            staleGenerationRejected = true;
            System.out.println("Stale generation rejected: " + e.getMessage());
        }

        if (!staleGenerationRejected) {
            throw new AssertionError("Stale generation should be rejected");
        }
        JoinGroupResult rejoin = coordinator.joinGroup(groupId, "consumer-A", partitionCount);
        System.out.println("Consumer-A rejoined with generation: " + rejoin.generation());
        if (rejoin.generation() != 3) {
            throw new AssertionError("Expected rejoin generation to be 3");
        }
        JoinGroupResult syncResult = coordinator.syncGroup(groupId, "consumer-A", rejoin.generation());
        System.out.println("Consumer-A assignment after rejoin: " + syncResult.partitions());

        if (syncResult.generation() != 3) {
            throw new AssertionError("SYNC_GROUP returned incorrect generation");
        }
        if (syncResult.partitions() == null) {
            throw new AssertionError("Assignment cannot be null");
        }
        System.out.println();
        System.out.println("CONSUMER GROUP REJOIN VERIFIED!");
    }
}
package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroupCoordinator;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.JoinGroupResult;

import java.util.List;

public class SyncGroupCoordinatorTest {

    public static void main(String[] args) {

        System.out.println("=== SYNC GROUP COORDINATOR TEST ===");
        ConsumerGroupManager groupManager = new ConsumerGroupManager();
        ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator(groupManager);
        JoinGroupResult resultA = coordinator.joinGroup("orders-group", "consumer-A", 4);
        System.out.println("Consumer-A JOIN assignment: " + resultA.partitions());
        JoinGroupResult resultB = coordinator.joinGroup("orders-group", "consumer-B", 4);
        System.out.println("Consumer-B JOIN assignment: " + resultB.partitions());
        JoinGroupResult syncedA = coordinator.syncGroup("orders-group", "consumer-A", 0);
        System.out.println("Consumer-A SYNC assignment: " + syncedA.partitions());
        JoinGroupResult syncedB = coordinator.syncGroup("orders-group", "consumer-B", 0);
        System.out.println("Consumer-B SYNC assignment: " + syncedB.partitions());
        if (!syncedA.partitions().equals(List.of(0, 2))) {
            throw new AssertionError("Consumer-A assignment mismatch");
        }
        if (!syncedB.partitions().equals(List.of(1, 3))) {
            throw new AssertionError("Consumer-B assignment mismatch");
        }
        System.out.println();
        System.out.println("SYNC_GROUP coordinator verified successfully!");
    }
}
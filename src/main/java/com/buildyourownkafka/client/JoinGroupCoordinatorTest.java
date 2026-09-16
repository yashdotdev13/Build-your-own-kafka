package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroup;
import com.buildyourownkafka.broker.ConsumerGroupCoordinator;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.ConsumerGroupState;
import com.buildyourownkafka.broker.JoinGroupResult;

import java.util.List;

public class JoinGroupCoordinatorTest {

    public static void main(String[] args) {

        System.out.println("=== JOIN GROUP COORDINATOR TEST ===");
        ConsumerGroupManager manager = new ConsumerGroupManager();
        ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator(manager);
        JoinGroupResult result = coordinator.joinGroup("orders-group", "consumer-A", 4);
        System.out.println("Member ID: " + result.memberId());
        System.out.println("Generation: " + result.generation());
        System.out.println("Partitions: " + result.partitions());
        if (!result.memberId().equals("consumer-A")) {
            throw new RuntimeException("Incorrect member ID");
        }
        if (result.generation() != 0) {
            throw new RuntimeException("Initial generation should be 0");
        }
        if (!result.partitions().equals(List.of(0, 1, 2, 3))) {
            throw new RuntimeException("Incorrect partition assignment");
        }
        ConsumerGroup group = manager.getGroup("orders-group");
        if (group == null) {
            throw new RuntimeException("Group should exist");
        }
        if (!group.hasMember("consumer-A")) {
            throw new RuntimeException("Member should exist");
        }
        if (group.state() != ConsumerGroupState.STABLE) {
            throw new RuntimeException("Group should be STABLE");
        }
        System.out.println("Group state: " + group.state());
        System.out.println();
        System.out.println("JOIN_GROUP coordinator result " + "verified successfully!");
    }
}
package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroup;
import com.buildyourownkafka.broker.ConsumerGroupCoordinator;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.GroupMember;

import java.util.List;

public class ConsumerGroupFailureDetectionIntegrationTest {

    public static void main(String[] args) {

        System.out.println("=== CONSUMER GROUP FAILURE DETECTION INTEGRATION TEST ===");
        ConsumerGroupManager manager = new ConsumerGroupManager();
        ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator(manager, 10_000);
        ConsumerGroup group = manager.getOrCreateGroup("orders-group");
        group.addMember(new GroupMember("consumer-A", "orders-group", 1, List.of(0, 2), 100_000));
        group.addMember(new GroupMember("consumer-B", "orders-group", 1, List.of(1, 3), 90_000));
        List<String> expiredMembers = coordinator.findExpiredMembers("orders-group", 105_000);
        System.out.println("Expired members: " + expiredMembers);
        if (!expiredMembers.equals(List.of("consumer-B"))) {
            throw new RuntimeException("Only consumer-B should be expired");
        }
        if (!group.hasMember("consumer-B")) {
            throw new RuntimeException("Failure detection should not remove members");
        }
        if (group.memberCount() != 2) {
            throw new RuntimeException("Failure detection should not change membership");
        }
        System.out.println();
        System.out.println("Coordinator failure detection " + "integration verified successfully!");
    }
}
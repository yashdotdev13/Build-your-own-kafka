package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroup;
import com.buildyourownkafka.broker.ConsumerGroupCoordinator;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.ConsumerGroupState;
import com.buildyourownkafka.broker.PartitionAssignment;

import java.util.List;

public class ConsumerGroupCoordinatorTest {

    public static void main(String[] args) {

        System.out.println(
                "=== CONSUMER GROUP COORDINATOR TEST ==="
        );

        ConsumerGroupManager groupManager =
                new ConsumerGroupManager();

        ConsumerGroupCoordinator coordinator =
                new ConsumerGroupCoordinator(
                        groupManager
                );

        /*
         * Consumer A joins.
         */

        PartitionAssignment assignment =
                coordinator.joinGroup(
                        "orders-group",
                        "consumer-A",
                        4
                );

        ConsumerGroup group =
                groupManager.getGroup(
                        "orders-group"
                );

        System.out.println(
                "After consumer-A joins: "
                        + assignment.assignments()
        );

        System.out.println(
                "Group state: "
                        + group.state()
        );

        if (group.state()
                != ConsumerGroupState.STABLE) {

            throw new RuntimeException(
                    "Group should be STABLE after join"
            );
        }

        if (!assignment
                .partitionsFor("consumer-A")
                .equals(List.of(0, 1, 2, 3))) {

            throw new RuntimeException(
                    "Consumer-A should own all partitions"
            );
        }

        /*
         * Consumer B joins.
         *
         * This triggers a rebalance.
         */

        assignment =
                coordinator.joinGroup(
                        "orders-group",
                        "consumer-B",
                        4
                );

        group =
                groupManager.getGroup(
                        "orders-group"
                );

        System.out.println(
                "After consumer-B joins: "
                        + assignment.assignments()
        );

        System.out.println(
                "Group state: "
                        + group.state()
        );

        if (group.state()
                != ConsumerGroupState.STABLE) {

            throw new RuntimeException(
                    "Group should be STABLE after rebalance"
            );
        }

        if (!assignment
                .partitionsFor("consumer-A")
                .equals(List.of(0, 2))) {

            throw new RuntimeException(
                    "Incorrect assignment for consumer-A"
            );
        }

        if (!assignment
                .partitionsFor("consumer-B")
                .equals(List.of(1, 3))) {

            throw new RuntimeException(
                    "Incorrect assignment for consumer-B"
            );
        }

        /*
         * Consumer A leaves.
         *
         * Another rebalance happens.
         */

        assignment =
                coordinator.leaveGroup(
                        "orders-group",
                        "consumer-A",
                        4
                );

        group =
                groupManager.getGroup(
                        "orders-group"
                );

        System.out.println(
                "After consumer-A leaves: "
                        + assignment.assignments()
        );

        System.out.println(
                "Group state: "
                        + group.state()
        );

        if (group.state()
                != ConsumerGroupState.STABLE) {

            throw new RuntimeException(
                    "Group should be STABLE after member leaves"
            );
        }

        if (!assignment
                .partitionsFor("consumer-B")
                .equals(List.of(0, 1, 2, 3))) {

            throw new RuntimeException(
                    "Consumer-B should own all partitions"
            );
        }

        /*
         * Consumer B leaves.
         *
         * This removes the final member,
         * so the group itself is removed.
         */

        assignment =
                coordinator.leaveGroup(
                        "orders-group",
                        "consumer-B",
                        4
                );

        if (assignment != null) {

            throw new RuntimeException(
                    "Assignment should be null "
                            + "after final member leaves"
            );
        }

        if (groupManager.groupExists(
                "orders-group"
        )) {

            throw new RuntimeException(
                    "Group should not exist "
                            + "after final member leaves"
            );
        }

        if (coordinator.getAssignment(
                "orders-group"
        ) != null) {

            throw new RuntimeException(
                    "Assignment should be removed "
                            + "after group deletion"
            );
        }

        System.out.println();
        System.out.println(
                "ConsumerGroupCoordinator state "
                        + "transitions verified successfully!"
        );
    }
}
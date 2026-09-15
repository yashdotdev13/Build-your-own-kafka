package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroup;
import com.buildyourownkafka.broker.ConsumerGroupState;

public class ConsumerGroupTest {

    public static void main(String[] args) {

        System.out.println(
                "=== CONSUMER GROUP TEST ==="
        );

        ConsumerGroup group =
                new ConsumerGroup(
                        "payment-service"
                );

        /*
         * New groups should start EMPTY.
         */

        System.out.println(
                "Initial state: "
                        + group.state()
        );

        if (group.state()
                != ConsumerGroupState.EMPTY) {

            throw new RuntimeException(
                    "New group should start in EMPTY state"
            );
        }

        /*
         * Add members.
         */

        group.addMember(
                "consumer-1"
        );

        group.addMember(
                "consumer-2"
        );

        System.out.println(
                "Group ID: "
                        + group.groupId()
        );

        System.out.println(
                "Member count: "
                        + group.memberCount()
        );

        System.out.println(
                "Members: "
                        + group.members()
        );

        /*
         * Change state.
         */

        group.setState(
                ConsumerGroupState.PREPARING_REBALANCE
        );

        if (group.state()
                != ConsumerGroupState.PREPARING_REBALANCE) {

            throw new RuntimeException(
                    "Group should be in "
                            + "PREPARING_REBALANCE state"
            );
        }

        System.out.println(
                "State changed to: "
                        + group.state()
        );

        group.setState(
                ConsumerGroupState.COMPLETING_REBALANCE
        );

        if (group.state()
                != ConsumerGroupState.COMPLETING_REBALANCE) {

            throw new RuntimeException(
                    "Group should be in "
                            + "COMPLETING_REBALANCE state"
            );
        }

        System.out.println(
                "State changed to: "
                        + group.state()
        );

        group.setState(
                ConsumerGroupState.STABLE
        );

        if (group.state()
                != ConsumerGroupState.STABLE) {

            throw new RuntimeException(
                    "Group should be in STABLE state"
            );
        }

        System.out.println(
                "State changed to: "
                        + group.state()
        );

        /*
         * Remove members.
         */

        group.removeMember(
                "consumer-1"
        );

        if (group.memberCount() != 1) {

            throw new RuntimeException(
                    "Expected one remaining member"
            );
        }

        group.removeMember(
                "consumer-2"
        );

        if (group.memberCount() != 0) {

            throw new RuntimeException(
                    "Expected zero members"
            );
        }

        /*
         * State is intentionally not
         * automatically changed by
         * removeMember().
         */

        if (group.state()
                != ConsumerGroupState.STABLE) {

            throw new RuntimeException(
                    "Group state should remain STABLE"
            );
        }

        System.out.println();
        System.out.println(
                "ConsumerGroup state management "
                        + "verified successfully!"
        );
    }
}
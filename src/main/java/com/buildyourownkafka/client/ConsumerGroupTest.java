package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroup;

public class ConsumerGroupTest {

    public static void main(String[] args) {

        System.out.println(
                "=== CONSUMER GROUP TEST ==="
        );

        ConsumerGroup group =
                new ConsumerGroup(
                        "payment-service"
                );

        System.out.println(
                "Group ID: "
                        + group.groupId()
        );

        /*
         * Add consumers.
         */

        group.addMember(
                "consumer-1"
        );

        group.addMember(
                "consumer-2"
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
         * Verify membership.
         */

        if (!group.hasMember("consumer-1")) {

            throw new RuntimeException(
                    "consumer-1 should be a member"
            );
        }

        if (!group.hasMember("consumer-2")) {

            throw new RuntimeException(
                    "consumer-2 should be a member"
            );
        }

        /*
         * Verify duplicate registration.
         */

        group.addMember(
                "consumer-1"
        );

        if (group.memberCount() != 2) {

            throw new RuntimeException(
                    "Duplicate member was added"
            );
        }

        /*
         * Remove consumer.
         */

        group.removeMember(
                "consumer-1"
        );

        if (group.hasMember("consumer-1")) {

            throw new RuntimeException(
                    "consumer-1 should have been removed"
            );
        }

        if (group.memberCount() != 1) {

            throw new RuntimeException(
                    "Unexpected member count"
            );
        }

        System.out.println();
        System.out.println(
                "ConsumerGroup model verified successfully!"
        );
    }
}
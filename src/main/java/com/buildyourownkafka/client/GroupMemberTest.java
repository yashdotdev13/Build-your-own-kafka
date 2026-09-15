package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.GroupMember;

import java.util.List;

public class GroupMemberTest {

    public static void main(String[] args) {

        System.out.println(
                "=== GROUP MEMBER TEST ==="
        );

        GroupMember member =
                new GroupMember(
                        "consumer-A",
                        "orders-group",
                        1,
                        List.of(0, 2)
                );

        System.out.println(
                "Member ID: "
                        + member.memberId()
        );

        System.out.println(
                "Group ID: "
                        + member.groupId()
        );

        System.out.println(
                "Generation: "
                        + member.generation()
        );

        System.out.println(
                "Assigned partitions: "
                        + member.assignedPartitions()
        );

        if (!member.memberId()
                .equals("consumer-A")) {

            throw new RuntimeException(
                    "Incorrect member ID"
            );
        }

        if (!member.groupId()
                .equals("orders-group")) {

            throw new RuntimeException(
                    "Incorrect group ID"
            );
        }

        if (member.generation() != 1) {

            throw new RuntimeException(
                    "Incorrect generation"
            );
        }

        if (!member.assignedPartitions()
                .equals(List.of(0, 2))) {

            throw new RuntimeException(
                    "Incorrect partition assignment"
            );
        }

        System.out.println();

        System.out.println(
                "GroupMember metadata verified successfully!"
        );
    }
}
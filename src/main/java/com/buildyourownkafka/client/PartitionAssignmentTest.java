package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.PartitionAssignment;

import java.util.List;

public class PartitionAssignmentTest {

    public static void main(String[] args) {

        System.out.println(
                "=== PARTITION ASSIGNMENT TEST ==="
        );

        List<String> members =
                List.of(
                        "consumer-A",
                        "consumer-B"
                );

        PartitionAssignment assignment =
                new PartitionAssignment(
                        members,
                        4
                );

        System.out.println(
                "Assignments: "
                        + assignment.assignments()
        );

        /*
         * Expected:
         *
         * consumer-A -> [0, 2]
         * consumer-B -> [1, 3]
         */

        List<Integer> consumerA =
                assignment.partitionsFor(
                        "consumer-A"
                );

        List<Integer> consumerB =
                assignment.partitionsFor(
                        "consumer-B"
                );

        if (!consumerA.equals(
                List.of(0, 2)
        )) {

            throw new RuntimeException(
                    "Incorrect assignment for consumer-A: "
                            + consumerA
            );
        }

        if (!consumerB.equals(
                List.of(1, 3)
        )) {

            throw new RuntimeException(
                    "Incorrect assignment for consumer-B: "
                            + consumerB
            );
        }

        /*
         * Verify all partitions were assigned.
         */

        if (assignment.partitionCount()
                != 4) {

            throw new RuntimeException(
                    "Not all partitions were assigned"
            );
        }

        /*
         * Verify more consumers than partitions.
         */

        PartitionAssignment unevenAssignment =
                new PartitionAssignment(
                        List.of(
                                "consumer-A",
                                "consumer-B",
                                "consumer-C",
                                "consumer-D",
                                "consumer-E"
                        ),
                        3
                );

        if (!unevenAssignment
                .partitionsFor("consumer-A")
                .equals(List.of(0))) {

            throw new RuntimeException(
                    "Incorrect uneven assignment"
            );
        }

        if (!unevenAssignment
                .partitionsFor("consumer-B")
                .equals(List.of(1))) {

            throw new RuntimeException(
                    "Incorrect uneven assignment"
            );
        }

        if (!unevenAssignment
                .partitionsFor("consumer-C")
                .equals(List.of(2))) {

            throw new RuntimeException(
                    "Incorrect uneven assignment"
            );
        }

        if (!unevenAssignment
                .partitionsFor("consumer-D")
                .isEmpty()) {

            throw new RuntimeException(
                    "Consumer-D should have no partitions"
            );
        }

        System.out.println();
        System.out.println(
                "Partition assignment verified successfully!"
        );
    }
}
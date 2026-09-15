package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.PartitionAssignment;
import com.buildyourownkafka.broker.PartitionAssigner;

import java.util.List;

public class PartitionAssignerTest {

    public static void main(String[] args) {

        System.out.println("=== PARTITION ASSIGNER TEST ===");

        PartitionAssigner assigner = new PartitionAssigner();
        List<String> members = List.of("consumer-B", "consumer-A");
        PartitionAssignment assignment = assigner.assign(members, 4);
        System.out.println("Assignments: " + assignment.assignments());
        if (!assignment.partitionsFor("consumer-A").equals(List.of(0, 2))) {
            throw new RuntimeException("Incorrect assignment for consumer-A");
        }
        if (!assignment.partitionsFor("consumer-B").equals(List.of(1, 3))) {
            throw new RuntimeException("Incorrect assignment for consumer-B");
        }
        if (assignment.partitionCount() != 4) {
            throw new RuntimeException("Expected all 4 partitions to be assigned");
        }
        PartitionAssignment secondAssignment = assigner.assign(List.of("consumer-A", "consumer-B"), 4);
        if (!assignment.assignments().equals(secondAssignment.assignments())) {
            throw new RuntimeException("Assignment is not deterministic");
        }
        PartitionAssignment unevenAssignment = assigner.assign(List.of("consumer-E", "consumer-D", "consumer-C", "consumer-B", "consumer-A"), 3);
        if (!unevenAssignment.partitionsFor("consumer-A").equals(List.of(0))) {
            throw new RuntimeException("Incorrect assignment for consumer-A");
        }
        if (!unevenAssignment.partitionsFor("consumer-B").equals(List.of(1))) {
            throw new RuntimeException("Incorrect assignment for consumer-B");
        }
        if (!unevenAssignment.partitionsFor("consumer-C").equals(List.of(2))) {
            throw new RuntimeException("Incorrect assignment for consumer-C");
        }
        if (!unevenAssignment.partitionsFor("consumer-D").isEmpty()) {
            throw new RuntimeException("consumer-D should have no partitions");
        }
        if (!unevenAssignment.partitionsFor("consumer-E").isEmpty()) {
            throw new RuntimeException("consumer-E should have no partitions");
        }
        System.out.println();
        System.out.println("PartitionAssigner verified successfully!");
    }
}
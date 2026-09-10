package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.Partition;
import com.buildyourownkafka.broker.Record;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class PartitionTest {

    public static void main(String[] args) {

        Partition partition = new Partition(0);

        // Append records
        Record first = partition.append("Hello".getBytes(StandardCharsets.UTF_8));
        Record second = partition.append("Kafka".getBytes(StandardCharsets.UTF_8));
        Record third = partition.append("World".getBytes(StandardCharsets.UTF_8));

        // Print offsets
        System.out.println("First offset: " + first.offset());
        System.out.println("Second offset: " + second.offset());
        System.out.println("Third offset: " + third.offset());
        System.out.println("Next offset: " + partition.nextOffset());
        System.out.println("Partition size: " + partition.size());

        // Read a single record
        Record record = partition.read(1);
        System.out.println("Record at offset 1: " + new String(record.value(), StandardCharsets.UTF_8));

        // Read from an offset
        List<Record> records = partition.readFrom(1);
        System.out.println("Records from offset 1:");

        for (Record r : records) {
            System.out.println("Offset " + r.offset() + " -> " + new String(r.value(), StandardCharsets.UTF_8));
        }
    }
}
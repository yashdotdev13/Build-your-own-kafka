package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.PartitionLog;
import com.buildyourownkafka.broker.Record;

import java.nio.file.Files;
import java.nio.file.Path;

public class PartitionLogSegmentTest {

    public static void main(String[] args) throws Exception {

        Path directory = Path.of("data", "partition-log-test");
        Files.deleteIfExists(directory.resolve("segment-0.log"));
        PartitionLog log = new PartitionLog(directory);
        log.append(new Record(0, "order-123".getBytes()));
        log.append(new Record(1, "order-456".getBytes()));
        log.append(new Record(2, "order-789".getBytes()));
        System.out.println("Next offset: " + log.nextOffset());
        Record record = log.read(1);
        if (record != null) {
            System.out.println("Read offset " + record.offset() + " -> " + new String(record.value()));
        }
        System.out.println("Segment file exists: " + Files.exists(directory.resolve("segment-0.log")));
    }
}
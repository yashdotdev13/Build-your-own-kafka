package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.PartitionLog;
import com.buildyourownkafka.broker.Record;

import java.nio.file.Files;
import java.nio.file.Path;

public class PartitionLogTest {

    public static void main(String[] args) throws Exception {

        Path logFile = Path.of("data", "test-topic", "partition-0", "partition.log");
        Files.deleteIfExists(logFile);
        System.out.println("=== PROCESS 1: WRITING ===");
        PartitionLog log = new PartitionLog(logFile);
        log.append(new Record(0, "order-123".getBytes()));
        log.append(new Record(1, "order-456".getBytes()));
        log.append(new Record(2, "order-789".getBytes()));
        System.out.println("Records written to disk.");
    }
}
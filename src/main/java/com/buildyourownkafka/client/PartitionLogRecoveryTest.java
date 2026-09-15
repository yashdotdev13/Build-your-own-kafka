package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.PartitionLog;
import com.buildyourownkafka.broker.Record;

import java.nio.file.Path;

public class PartitionLogRecoveryTest {

    public static void main(String[] args) {

        Path logDirectory = Path.of("data", "test-topic", "partition-0");
        System.out.println("=== PROCESS 2: RECOVERY ===");
        PartitionLog log = new PartitionLog(logDirectory);
        System.out.println("Recovered next offset: " + log.nextOffset());
        Record record0 = log.read(0);
        Record record1 = log.read(1);
        Record record2 = log.read(2);
        printRecord(record0);
        printRecord(record1);
        printRecord(record2);
    }
    private static void printRecord(Record record) {
        if (record == null) {
            System.out.println("Record not found");
            return;
        }
        System.out.println("Recovered offset " + record.offset() + " -> " + new String(record.value()));
    }
}
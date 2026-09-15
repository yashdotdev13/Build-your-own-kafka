package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.Record;

import java.util.List;

public class ConsumerPollTest {

    public static void main(String[] args) throws Exception {

        try (Consumer consumer = new Consumer("localhost", 9092, "orders", 1, 0)) {
            System.out.println("Consumer connected.");
            System.out.println("Starting offset: " + consumer.currentOffset());

            // First poll
            List<Record> firstPoll = consumer.poll();

            System.out.println();
            System.out.println("=== FIRST POLL ===");
            System.out.println("Records received: " + firstPoll.size());
            for (Record record : firstPoll) {
                System.out.println("Offset " + record.offset() + " -> " + new String(record.value()));
            }
            System.out.println("Current offset: " + consumer.currentOffset());
            List<Record> secondPoll = consumer.poll();
            System.out.println();
            System.out.println("=== SECOND POLL ===");
            System.out.println("Records received: " + secondPoll.size());
            for (Record record : secondPoll) {
                System.out.println("Offset " + record.offset() + " -> " + new String(record.value()));
            }
            System.out.println("Current offset: " + consumer.currentOffset());
        }
    }
}
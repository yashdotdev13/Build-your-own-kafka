package com.buildyourownkafka.client;

public class ConsumerTest {

    public static void main(String[] args) {

        Consumer consumer =
                new Consumer(
                        "orders",
                        1,
                        0
                );

        System.out.println(
                "Topic: "
                        + consumer.topic()
        );

        System.out.println(
                "Partition: "
                        + consumer.partition()
        );

        System.out.println(
                "Current offset: "
                        + consumer.currentOffset()
        );

        consumer.advanceOffset(3);

        System.out.println(
                "Current offset after advance: "
                        + consumer.currentOffset()
        );
    }
}
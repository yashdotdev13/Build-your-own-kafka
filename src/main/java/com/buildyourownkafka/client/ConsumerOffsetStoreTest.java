package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerOffsetStore;

import java.nio.file.Path;

public class ConsumerOffsetStoreTest {

    public static void main(String[] args) {

        Path offsetDirectory =
                Path.of(
                        "data",
                        "test-offsets"
                );

        ConsumerOffsetStore store =
                new ConsumerOffsetStore(
                        offsetDirectory
                );

        System.out.println(
                "=== CONSUMER OFFSET STORE ==="
        );

        store.commit(
                "payment-service",
                "orders",
                1,
                4
        );

        System.out.println(
                "Committed offset: 4"
        );

        long offset =
                store.fetch(
                        "payment-service",
                        "orders",
                        1
                );

        System.out.println(
                "Fetched offset: "
                        + offset
        );

        store.commit(
                "payment-service",
                "orders",
                1,
                7
        );

        System.out.println(
                "Committed offset: 7"
        );

        System.out.println(
                "Fetched offset: "
                        + store.fetch(
                        "payment-service",
                        "orders",
                        1
                )
        );

        System.out.println();

        System.out.println(
                "Different group:"
        );

        System.out.println(
                "Fetched offset: "
                        + store.fetch(
                        "analytics-service",
                        "orders",
                        1
                )
        );
    }
}
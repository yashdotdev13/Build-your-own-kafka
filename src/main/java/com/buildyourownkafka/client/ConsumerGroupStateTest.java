package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroupState;

public class ConsumerGroupStateTest {

    public static void main(String[] args) {

        System.out.println(
                "=== CONSUMER GROUP STATE TEST ==="
        );

        ConsumerGroupState state =
                ConsumerGroupState.EMPTY;

        System.out.println(
                "Initial state: "
                        + state
        );

        if (state != ConsumerGroupState.EMPTY) {

            throw new RuntimeException(
                    "Initial state should be EMPTY"
            );
        }

        state =
                ConsumerGroupState.PREPARING_REBALANCE;

        System.out.println(
                "Preparing state: "
                        + state
        );

        if (state
                != ConsumerGroupState.PREPARING_REBALANCE) {

            throw new RuntimeException(
                    "State should be PREPARING_REBALANCE"
            );
        }

        state =
                ConsumerGroupState.COMPLETING_REBALANCE;

        System.out.println(
                "Completing state: "
                        + state
        );

        if (state
                != ConsumerGroupState.COMPLETING_REBALANCE) {

            throw new RuntimeException(
                    "State should be COMPLETING_REBALANCE"
            );
        }

        state =
                ConsumerGroupState.STABLE;

        System.out.println(
                "Stable state: "
                        + state
        );

        if (state != ConsumerGroupState.STABLE) {

            throw new RuntimeException(
                    "State should be STABLE"
            );
        }

        System.out.println();
        System.out.println(
                "ConsumerGroupState verified successfully!"
        );
    }
}
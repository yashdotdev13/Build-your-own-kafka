package com.buildyourownkafka.broker;

public enum ConsumerGroupState {

    EMPTY,

    PREPARING_REBALANCE,

    COMPLETING_REBALANCE,

    STABLE
}
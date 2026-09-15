package com.buildyourownkafka.broker;

public class ConsumerGroupException
        extends RuntimeException {

    public ConsumerGroupException(
            String message
    ) {
        super(message);
    }
}
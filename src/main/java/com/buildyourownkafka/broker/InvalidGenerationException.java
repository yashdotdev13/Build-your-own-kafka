package com.buildyourownkafka.broker;

public class InvalidGenerationException extends ConsumerGroupException {

    public InvalidGenerationException(String message) {
        super(message);
    }
}
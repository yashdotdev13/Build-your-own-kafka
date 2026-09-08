package com.buildyourownkafka.broker;

public class Topic {

    private final String name;

    public Topic(String name) {

        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException(
                    "Topic name cannot be empty"
            );
        }

        this.name = name;
    }

    public String name() {
        return name;
    }
}
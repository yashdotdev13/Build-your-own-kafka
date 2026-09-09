package com.buildyourownkafka.protocol;

import java.nio.charset.StandardCharsets;

public record Frame(byte[] payload) {

    public Frame {
        if (payload == null) {
            throw new IllegalArgumentException("payload cannot be null");
        }
    }

    public Frame(String message) {
        this(message.getBytes(StandardCharsets.UTF_8));
    }

    public String payloadAsString() {
        return new String(payload, StandardCharsets.UTF_8);
    }

    public int length() {
        return payload.length;
    }
}
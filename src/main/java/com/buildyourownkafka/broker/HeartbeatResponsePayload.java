package com.buildyourownkafka.broker;

import java.nio.charset.StandardCharsets;

public record HeartbeatResponsePayload(
        String memberId,
        int generation
) {

    public HeartbeatResponsePayload {

        if (memberId == null || memberId.isBlank()) {
            throw new IllegalArgumentException(
                    "Member ID cannot be blank"
            );
        }

        if (generation < 0) {
            throw new IllegalArgumentException(
                    "Generation cannot be negative"
            );
        }
    }

    public byte[] encode() {

        return (
                memberId
                        + ":"
                        + generation
        ).getBytes(StandardCharsets.UTF_8);
    }

    public static HeartbeatResponsePayload decode(
            byte[] payload
    ) {

        if (payload == null) {
            throw new IllegalArgumentException(
                    "Payload cannot be null"
            );
        }

        String value =
                new String(
                        payload,
                        StandardCharsets.UTF_8
                );

        String[] parts =
                value.split(":", -1);

        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid heartbeat response payload"
            );
        }

        return new HeartbeatResponsePayload(
                parts[0],
                Integer.parseInt(parts[1])
        );
    }
}
package com.buildyourownkafka.protocol;

import java.nio.charset.StandardCharsets;

public final class ProtocolErrors {

    public static final String INTERNAL_ERROR_MESSAGE =
            "Internal server error";

    private ProtocolErrors() {
    }

    public static Response clientError(int correlationId, String message) {
        String safeMessage = message == null || message.isBlank()
                ? "Invalid request"
                : message;

        return new Response(
                correlationId,
                Response.ERROR,
                safeMessage.getBytes(StandardCharsets.UTF_8)
        );
    }

    public static Response internalError(int correlationId) {
        return new Response(
                correlationId,
                Response.ERROR,
                INTERNAL_ERROR_MESSAGE.getBytes(StandardCharsets.UTF_8)
        );
    }
}

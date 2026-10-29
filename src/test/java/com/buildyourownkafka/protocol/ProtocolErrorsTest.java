package com.buildyourownkafka.protocol;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ProtocolErrorsTest {

    @Test
    void shouldCreateClientErrorUsingUtf8AndPreserveCorrelationId() {
        Response response = ProtocolErrors.clientError(42, "Topic inválido");

        assertEquals(42, response.correlationId());
        assertEquals(Response.ERROR, response.status());
        assertEquals(
                "Topic inválido",
                new String(response.payload(), StandardCharsets.UTF_8)
        );
    }

    @Test
    void shouldUseFallbackForBlankClientErrorMessage() {
        Response response = ProtocolErrors.clientError(43, " ");

        assertEquals(
                "Invalid request",
                new String(response.payload(), StandardCharsets.UTF_8)
        );
    }

    @Test
    void shouldNotExposeInternalExceptionDetails() {
        Response response = ProtocolErrors.internalError(44);

        assertEquals(44, response.correlationId());
        assertEquals(Response.ERROR, response.status());
        assertEquals(
                "Internal server error",
                new String(response.payload(), StandardCharsets.UTF_8)
        );
    }
}

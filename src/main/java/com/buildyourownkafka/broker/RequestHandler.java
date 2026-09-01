package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.nio.charset.StandardCharsets;

public class RequestHandler {

    public Response handle(Request request) {

        if (request.type() == Request.PING) {

            return new Response(
                    request.correlationId(),
                    Response.SUCCESS,
                    "PONG".getBytes(
                            StandardCharsets.UTF_8
                    )
            );
        }

        return new Response(
                request.correlationId(),
                Response.ERROR,
                "Unknown request type"
                        .getBytes(StandardCharsets.UTF_8)
        );
    }
}
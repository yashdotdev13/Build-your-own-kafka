package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

import java.nio.charset.StandardCharsets;

public class PingRequestHandler implements RequestHandler {

    @Override
    public Response handle(Request request) {
        return new Response(request.correlationId(), Response.SUCCESS, "PONG".getBytes(StandardCharsets.UTF_8));
    }
}
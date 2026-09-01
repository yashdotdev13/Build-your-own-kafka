package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

public interface RequestHandler {

    Response handle(Request request);
}
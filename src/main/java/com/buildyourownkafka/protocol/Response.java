package com.buildyourownkafka.protocol;

public record Response(

        int correlationId,
        int status,
        byte[] payload
){
    public static final int SUCCESS = 0;
    public static final int ERROR = 1;

    public Response {
        if(payload == null){
            throw new NullPointerException("Payload cannot be null");
        }
    }
}
package com.buildyourownkafka.protocol;

public record Request (
        int type,
        short version,
        int correlationId,
        byte[] payload
){

    public static final int PING = 1;

    public Request{
        if(payload == null){
            throw new IllegalArgumentException("Payload cannot be null");
        }
    }
}
package com.buildyourownkafka.protocol;

public record Request(
        int type,
        short version,
        int correlationId,
        byte[] payload
) {

    public static final int PING = 1;
    public static final int CREATE_TOPIC = 2;
    public static final int PRODUCE = 3;
    public static final int FETCH = 4;
    public static final int COMMIT_OFFSET = 5;
    public static final int FETCH_OFFSET = 6;
    public static final int JOIN_GROUP = 7;
}
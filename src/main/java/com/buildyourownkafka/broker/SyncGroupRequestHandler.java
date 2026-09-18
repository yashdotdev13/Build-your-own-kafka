package com.buildyourownkafka.broker;

import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

public class SyncGroupRequestHandler implements RequestHandler {

    private final ConsumerGroupCoordinator coordinator;

    public SyncGroupRequestHandler(
            ConsumerGroupCoordinator coordinator
    ) {
        if (coordinator == null) {
            throw new IllegalArgumentException(
                    "Consumer group coordinator cannot be null"
            );
        }

        this.coordinator = coordinator;
    }

    @Override
    public Response handle(Request request) {

        try {

            SyncGroupRequestPayload payload =
                    SyncGroupRequestPayload.decode(
                            request.payload()
                    );

            System.out.println(
                    "Decoded SYNC_GROUP: "
                            + "group=" + payload.groupId()
                            + ", member=" + payload.memberId()
                            + ", generation=" + payload.generation()
            );

            JoinGroupResult result =
                    coordinator.syncGroup(
                            payload.groupId(),
                            payload.memberId(),
                            payload.generation()
                    );

            SyncGroupResponsePayload responsePayload =
                    new SyncGroupResponsePayload(
                            result.memberId(),
                            result.generation(),
                            result.partitions()
                    );

            return new Response(
                    request.correlationId(),
                    Response.SUCCESS,
                    responsePayload.encode()
            );

        } catch (IllegalArgumentException e) {

            System.err.println(
                    "Failed to process SYNC_GROUP: "
                            + e.getMessage()
            );

            return new Response(
                    request.correlationId(),
                    Response.ERROR,
                    e.getMessage().getBytes()
            );

        } catch (ConsumerGroupException e) {

            System.err.println(
                    "SYNC_GROUP group error: "
                            + e.getMessage()
            );

            return new Response(
                    request.correlationId(),
                    Response.ERROR,
                    e.getMessage().getBytes()
            );

        } catch (Exception e) {

            System.err.println(
                    "Unexpected SYNC_GROUP error: "
                            + e.getMessage()
            );

            return new Response(
                    request.correlationId(),
                    Response.ERROR,
                    "Internal SYNC_GROUP error".getBytes()
            );
        }
    }
}
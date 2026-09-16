package com.buildyourownkafka.client;

import java.nio.file.Files;
import java.nio.file.Path;

import com.buildyourownkafka.broker.ConsumerGroupCoordinator;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.ConsumerOffsetStore;
import com.buildyourownkafka.broker.JoinGroupRequestHandler;
import com.buildyourownkafka.broker.JoinGroupRequestPayload;
import com.buildyourownkafka.broker.LeaveGroupRequestPayload;
import com.buildyourownkafka.broker.LeaveGroupResponsePayload;
import com.buildyourownkafka.broker.RequestDispatcher;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

public class LeaveGroupDispatcherTest {

    public static void main(String[] args) throws Exception {

        System.out.println(
                "=== LEAVE GROUP DISPATCHER TEST ==="
        );

        /*
         * Create consumer group infrastructure.
         */

        ConsumerGroupManager groupManager =
                new ConsumerGroupManager();

        ConsumerGroupCoordinator coordinator =
                new ConsumerGroupCoordinator(
                        groupManager
                );

        /*
         * Create two consumers.
         */

        JoinGroupRequestHandler joinHandler =
                new JoinGroupRequestHandler(
                        coordinator
                );

        JoinGroupRequestPayload joinA =
                new JoinGroupRequestPayload(
                        "orders-group",
                        "consumer-A",
                        4
                );

        joinHandler.handle(
                new Request(
                        Request.JOIN_GROUP,
                        (short) 1,
                        1,
                        joinA.encode()
                )
        );

        JoinGroupRequestPayload joinB =
                new JoinGroupRequestPayload(
                        "orders-group",
                        "consumer-B",
                        4
                );

        joinHandler.handle(
                new Request(
                        Request.JOIN_GROUP,
                        (short) 1,
                        2,
                        joinB.encode()
                )
        );

        System.out.println(
                "Members before leave: "
                        + groupManager
                        .getGroup("orders-group")
                        .memberCount()
        );

        /*
         * Create a temporary directory for
         * ConsumerOffsetStore.
         */

        Path offsetDirectory =
                Files.createTempDirectory(
                        "leave-group-test"
                );

        ConsumerOffsetStore offsetStore =
                new ConsumerOffsetStore(
                        offsetDirectory
                );

        /*
         * Create dispatcher.
         *
         * TopicManager is not required for this test
         * because we are only testing LEAVE_GROUP.
         */

        RequestDispatcher dispatcher =
                new RequestDispatcher(
                        null,
                        offsetStore,
                        coordinator
                );

        /*
         * Create LEAVE_GROUP request payload.
         */

        LeaveGroupRequestPayload leavePayload =
                new LeaveGroupRequestPayload(
                        "orders-group",
                        "consumer-A",
                        4
                );

        /*
         * Create protocol request.
         */

        Request leaveRequest =
                new Request(
                        Request.LEAVE_GROUP,
                        (short) 1,
                        100,
                        leavePayload.encode()
                );

        /*
         * Dispatch LEAVE_GROUP.
         */

        Response response =
                dispatcher.dispatch(
                        leaveRequest
                );

        System.out.println(
                "Response correlation ID: "
                        + response.correlationId()
        );

        System.out.println(
                "Response status: "
                        + response.status()
        );

        /*
         * Verify correlation ID.
         */

        if (response.correlationId() != 100) {

            throw new AssertionError(
                    "Correlation ID mismatch"
            );
        }

        /*
         * Verify successful response.
         */

        if (response.status() != Response.SUCCESS) {

            throw new AssertionError(
                    "LEAVE_GROUP dispatch failed"
            );
        }

        /*
         * Decode response payload.
         */

        LeaveGroupResponsePayload responsePayload =
                LeaveGroupResponsePayload.decode(
                        response.payload()
                );

        System.out.println(
                "Left member: "
                        + responsePayload.memberId()
        );

        /*
         * Verify returned member ID.
         */

        if (!responsePayload.memberId()
                .equals("consumer-A")) {

            throw new AssertionError(
                    "Wrong member in response"
            );
        }

        /*
         * Verify consumer-A was removed.
         */

        if (groupManager
                .getGroup("orders-group")
                .memberCount() != 1) {

            throw new AssertionError(
                    "Consumer-A was not removed"
            );
        }

        /*
         * Verify consumer-B is still present.
         */

        if (!groupManager
                .getGroup("orders-group")
                .hasMember("consumer-B")) {

            throw new AssertionError(
                    "Consumer-B should still be present"
            );
        }

        /*
         * Verify group still exists.
         */

        if (!groupManager
                .groupExists("orders-group")) {

            throw new AssertionError(
                    "Consumer group should still exist"
            );
        }

        System.out.println();

        System.out.println(
                "LEAVE_GROUP dispatcher "
                        + "verified successfully!"
        );
    }
}
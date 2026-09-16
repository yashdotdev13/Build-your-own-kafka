package com.buildyourownkafka.client;

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

import java.nio.file.Files;
import java.nio.file.Path;

public class LeaveGroupFailureTest {

    public static void main(String[] args) throws Exception {

        System.out.println(
                "=== LEAVE GROUP INVALID MEMBER TEST ==="
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
         * Create two valid members.
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

        /*
         * Verify initial state.
         */

        int membersBefore =
                groupManager
                        .getGroup("orders-group")
                        .memberCount();

        System.out.println(
                "Members before invalid leave: "
                        + membersBefore
        );

        if (membersBefore != 2) {

            throw new AssertionError(
                    "Expected two members before test"
            );
        }

        /*
         * Create dispatcher.
         */

        Path offsetDirectory =
                Files.createTempDirectory(
                        "leave-group-failure-test"
                );

        ConsumerOffsetStore offsetStore =
                new ConsumerOffsetStore(
                        offsetDirectory
                );

        RequestDispatcher dispatcher =
                new RequestDispatcher(
                        null,
                        offsetStore,
                        coordinator
                );

        /*
         * Create LEAVE_GROUP request for
         * a member that does not exist.
         */

        LeaveGroupRequestPayload leavePayload =
                new LeaveGroupRequestPayload(
                        "orders-group",
                        "consumer-C",
                        4
                );

        Request leaveRequest =
                new Request(
                        Request.LEAVE_GROUP,
                        (short) 1,
                        100,
                        leavePayload.encode()
                );

        /*
         * Dispatch request.
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
         * Response must be ERROR.
         */

        if (response.status() != Response.ERROR) {

            throw new AssertionError(
                    "Invalid member should receive ERROR response"
            );
        }

        /*
         * Correlation ID must be preserved.
         */

        if (response.correlationId() != 100) {

            throw new AssertionError(
                    "Correlation ID mismatch"
            );
        }

        /*
         * Print error message.
         */

        String errorMessage =
                new String(response.payload());

        System.out.println(
                "Error message: "
                        + errorMessage
        );

        /*
         * Verify existing members were not removed.
         */

        int membersAfter =
                groupManager
                        .getGroup("orders-group")
                        .memberCount();

        System.out.println(
                "Members after invalid leave: "
                        + membersAfter
        );

        if (membersAfter != 2) {

            throw new AssertionError(
                    "Invalid leave modified group membership"
            );
        }

        /*
         * Verify consumer-A remains.
         */

        if (!groupManager
                .getGroup("orders-group")
                .hasMember("consumer-A")) {

            throw new AssertionError(
                    "Consumer-A should still be present"
            );
        }

        /*
         * Verify consumer-B remains.
         */

        if (!groupManager
                .getGroup("orders-group")
                .hasMember("consumer-B")) {

            throw new AssertionError(
                    "Consumer-B should still be present"
            );
        }

        /*
         * Verify invalid member was never added.
         */

        if (groupManager
                .getGroup("orders-group")
                .hasMember("consumer-C")) {

            throw new AssertionError(
                    "Consumer-C should not exist"
            );
        }

        System.out.println();

        System.out.println(
                "LEAVE_GROUP invalid member "
                        + "handling verified successfully!"
        );
    }
}
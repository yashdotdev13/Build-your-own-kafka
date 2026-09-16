package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroupCoordinator;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.LeaveGroupRequestHandler;
import com.buildyourownkafka.broker.LeaveGroupRequestPayload;
import com.buildyourownkafka.broker.LeaveGroupResponsePayload;
import com.buildyourownkafka.broker.PartitionAssignment;
import com.buildyourownkafka.broker.JoinGroupRequestPayload;
import com.buildyourownkafka.broker.JoinGroupRequestHandler;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

public class LeaveGroupRequestHandlerTest {

    public static void main(String[] args) throws Exception {

        System.out.println(
                "=== LEAVE GROUP REQUEST HANDLER TEST ==="
        );

        ConsumerGroupManager groupManager =
                new ConsumerGroupManager();

        ConsumerGroupCoordinator coordinator =
                new ConsumerGroupCoordinator(groupManager);

        /*
         * First create two members.
         */

        JoinGroupRequestHandler joinHandler =
                new JoinGroupRequestHandler(coordinator);

        JoinGroupRequestPayload joinA =
                new JoinGroupRequestPayload(
                        "orders-group",
                        "consumer-A",
                        4
                );

        Request joinRequestA =
                new Request(
                        Request.JOIN_GROUP,
                        (short) 1,
                        1,
                        joinA.encode()
                );

        joinHandler.handle(joinRequestA);

        JoinGroupRequestPayload joinB =
                new JoinGroupRequestPayload(
                        "orders-group",
                        "consumer-B",
                        4
                );

        Request joinRequestB =
                new Request(
                        Request.JOIN_GROUP,
                        (short) 1,
                        2,
                        joinB.encode()
                );

        joinHandler.handle(joinRequestB);

        System.out.println(
                "Members before leave: "
                        + groupManager
                        .getGroup("orders-group")
                        .memberCount()
        );

        /*
         * Now consumer-A leaves.
         */

        LeaveGroupRequestHandler leaveHandler =
                new LeaveGroupRequestHandler(coordinator);

        LeaveGroupRequestPayload leavePayload =
                new LeaveGroupRequestPayload(
                        "orders-group",
                        "consumer-A",
                        4
                );

        Request leaveRequest =
                new Request(
                        Request.LEAVE_GROUP,
                        (short) 1,
                        3,
                        leavePayload.encode()
                );

        Response response =
                leaveHandler.handle(leaveRequest);

        System.out.println(
                "Response correlation ID: "
                        + response.correlationId()
        );

        System.out.println(
                "Response status: "
                        + response.status()
        );

        if (response.status() != Response.SUCCESS) {
            throw new AssertionError(
                    "LEAVE_GROUP request failed"
            );
        }

        LeaveGroupResponsePayload responsePayload =
                LeaveGroupResponsePayload.decode(
                        response.payload()
                );

        System.out.println(
                "Left member: "
                        + responsePayload.memberId()
        );

        /*
         * Verify group state.
         */

        if (!responsePayload.memberId()
                .equals("consumer-A")) {

            throw new AssertionError(
                    "Incorrect member ID in response"
            );
        }

        if (!groupManager.groupExists("orders-group")) {
            throw new AssertionError(
                    "Group should still exist"
            );
        }

        if (groupManager
                .getGroup("orders-group")
                .memberCount() != 1) {

            throw new AssertionError(
                    "Consumer-A was not removed"
            );
        }

        if (!groupManager
                .getGroup("orders-group")
                .hasMember("consumer-B")) {

            throw new AssertionError(
                    "Consumer-B should still be present"
            );
        }

        PartitionAssignment assignment =
                coordinator.getAssignment(
                        "orders-group"
                );

        System.out.println(
                "Remaining assignment: "
                        + assignment.assignments()
        );

        if (assignment == null) {
            throw new AssertionError(
                    "Assignment should exist"
            );
        }

        System.out.println();
        System.out.println(
                "LEAVE_GROUP request handler "
                        + "verified successfully!"
        );
    }
}
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

public class LeaveGroupLastMemberTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== LEAVE GROUP LAST MEMBER TEST ===");
        ConsumerGroupManager groupManager = new ConsumerGroupManager();
        ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator(groupManager);
        JoinGroupRequestHandler joinHandler = new JoinGroupRequestHandler(coordinator);
        JoinGroupRequestPayload joinPayload = new JoinGroupRequestPayload("orders-group", "consumer-A", 4);
        Response joinResponse = joinHandler.handle(new Request(Request.JOIN_GROUP, (short) 1, 1, joinPayload.encode()));

        if (joinResponse.status() != Response.SUCCESS) {
            throw new AssertionError("JOIN_GROUP failed");
        }
        System.out.println("Group exists after JOIN: " + groupManager.groupExists("orders-group"));
        System.out.println("Members after JOIN: " + groupManager.getGroup("orders-group").memberCount());
        if (!groupManager.groupExists("orders-group")) {
            throw new AssertionError("Group should exist after JOIN");
        }

        if (groupManager.getGroup("orders-group").memberCount() != 1) {
            throw new AssertionError("Expected exactly one member");
        }
        Path offsetDirectory = Files.createTempDirectory("leave-group-last-member-test");
        ConsumerOffsetStore offsetStore = new ConsumerOffsetStore(offsetDirectory);
        RequestDispatcher dispatcher = new RequestDispatcher(null, offsetStore, coordinator);

        LeaveGroupRequestPayload leavePayload = new LeaveGroupRequestPayload("orders-group", "consumer-A", 4);
        Request leaveRequest = new Request(Request.LEAVE_GROUP, (short) 1, 100, leavePayload.encode());

        Response leaveResponse = dispatcher.dispatch(leaveRequest);
        System.out.println("Response correlation ID: " + leaveResponse.correlationId());
        System.out.println("Response status: " + leaveResponse.status());
        if (leaveResponse.status() != Response.SUCCESS) {
            throw new AssertionError("LEAVE_GROUP failed");
        }

        if (leaveResponse.correlationId() != 100) {
            throw new AssertionError("Correlation ID mismatch");
        }

        LeaveGroupResponsePayload responsePayload = LeaveGroupResponsePayload.decode(leaveResponse.payload());
        System.out.println("Left member: " + responsePayload.memberId());
        boolean groupExists = groupManager.groupExists("orders-group");
        System.out.println("Group exists after last member leaves: " + groupExists);

        if (groupExists) {
            throw new AssertionError("Group should be removed after last member leaves");
        }
        if (groupManager.groupCount() != 0) {
            throw new AssertionError("Expected zero consumer groups");
        }
        if (coordinator.getAssignment("orders-group") != null) {
            throw new AssertionError("Assignment should be removed " + "after last member leaves");
        }
        System.out.println();
        System.out.println("LEAVE_GROUP last member cleanup " + "verified successfully!");
    }
}
package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroupCoordinator;
import com.buildyourownkafka.broker.ConsumerGroupManager;
import com.buildyourownkafka.broker.GroupMember;
import com.buildyourownkafka.broker.HeartbeatRequestHandler;
import com.buildyourownkafka.broker.HeartbeatRequestPayload;
import com.buildyourownkafka.broker.HeartbeatResponsePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.Response;

public class HeartbeatRequestHandlerTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== HEARTBEAT REQUEST HANDLER TEST ===");
        ConsumerGroupManager groupManager = new ConsumerGroupManager();
        ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator(groupManager);

        String groupId = "orders-group";
        String memberId = "consumer-A";

        groupManager.getOrCreateGroup(groupId);
        groupManager.addMember(groupId, memberId);
        HeartbeatRequestPayload payload = new HeartbeatRequestPayload(groupId, memberId, 0);
        Request request = new Request(Request.HEARTBEAT, (short) 1, 100, payload.encode());
        HeartbeatRequestHandler handler = new HeartbeatRequestHandler(coordinator);
        Response response = handler.handle(request);
        System.out.println("Response status: " + response.status());
        if (response.status() != Response.SUCCESS) {
            throw new AssertionError("HEARTBEAT request failed: " + new String(response.payload()));
        }

        HeartbeatResponsePayload responsePayload = HeartbeatResponsePayload.decode(response.payload());
        System.out.println("Response member: " + responsePayload.memberId());
        System.out.println("Response generation: " + responsePayload.generation());

        if (!memberId.equals(responsePayload.memberId())) {
            throw new AssertionError("Member ID mismatch");
        }
        if (responsePayload.generation() != 0) {
            throw new AssertionError("Generation mismatch");
        }
        System.out.println("HEARTBEAT REQUEST HANDLER VERIFIED!");
    }
}
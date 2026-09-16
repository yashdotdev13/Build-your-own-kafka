package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.ConsumerGroupFailureDetector;
import com.buildyourownkafka.broker.GroupMember;

import java.util.List;

public class ConsumerGroupFailureDetectorTest {

    public static void main(String[] args) {

        System.out.println("=== CONSUMER GROUP FAILURE DETECTOR TEST ===");

        long sessionTimeout = 10_000;
        ConsumerGroupFailureDetector detector = new ConsumerGroupFailureDetector(sessionTimeout);

        GroupMember member = new GroupMember("consumer-A", "orders-group", 1, List.of(0, 2), 100_000);
        long lastHeartbeat = member.lastHeartbeat();

        System.out.println("Last heartbeat: " + lastHeartbeat);
        long currentTime = 105_000;
        boolean expired = detector.isExpired(member, currentTime);
        System.out.println("Expired after 5 seconds: " + expired);
        if (expired) {
            throw new RuntimeException("Member should still be alive");
        }
        currentTime = 110_000;
        expired = detector.isExpired(member, currentTime);
        System.out.println("Expired at exactly 10 seconds: " + expired);

        if (expired) {
            throw new RuntimeException("Member should not expire at exact timeout boundary");
        }
        currentTime = 110_001;
        expired = detector.isExpired(member, currentTime);
        System.out.println("Expired after timeout: " + expired);

        if (!expired) {
            throw new RuntimeException("Member should be expired");
        }
        if (detector.sessionTimeoutMillis() != sessionTimeout) {
            throw new RuntimeException("Incorrect session timeout");
        }
        System.out.println();
        System.out.println("Consumer group failure detection " + "verified successfully!");
    }
}
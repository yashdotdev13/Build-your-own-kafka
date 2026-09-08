package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.Topic;
import com.buildyourownkafka.broker.TopicManager;

public class TopicTest {

    public static void main(String[] args) {

        TopicManager topicManager =
                new TopicManager();

        Topic orders =
                topicManager.createTopic("orders");

        System.out.println(
                "Created topic: " + orders.name()
        );

        Topic payments =
                topicManager.createTopic("payments");

        System.out.println(
                "Created topic: " + payments.name()
        );

        System.out.println(
                "Topic count: "
                        + topicManager.getAllTopics().size()
        );

        System.out.println(
                "Orders exists: "
                        + topicManager.topicExists("orders")
        );

        System.out.println(
                "Payments exists: "
                        + topicManager.topicExists("payments")
        );
    }
}
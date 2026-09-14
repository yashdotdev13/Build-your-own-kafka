package com.buildyourownkafka.client;

import java.nio.file.Path;
import com.buildyourownkafka.broker.Topic;
import com.buildyourownkafka.broker.TopicManager;

public class TopicTest {

    public static void main(String[] args) {

        TopicManager topicManager =
                new TopicManager(Path.of("data", "test-topics"));
        Topic orders = topicManager.createTopic("orders", 3);
        Topic payments = topicManager.createTopic("payments", 2);
        System.out.println("Created topic: " + orders.name());
        System.out.println("Orders partitions: " + orders.partitionCount());
        System.out.println("Created topic: " + payments.name());
        System.out.println("Payments partitions: " + payments.partitionCount());
        System.out.println("Topic count: " + topicManager.getAllTopics().size());
        System.out.println("Orders partition 0: " + orders.getPartition(0).id());
        System.out.println("Orders partition 1: " + orders.getPartition(1).id());

        System.out.println("Orders partition 2: " + orders.getPartition(2).id());
    }
}
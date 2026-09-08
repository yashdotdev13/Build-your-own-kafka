package com.buildyourownkafka.broker;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TopicManager {

    private final Map<String, Topic> topics =
            new ConcurrentHashMap<>();

    public Topic createTopic(String name) {
        Topic topic = new Topic(name);

        Topic existing =
                topics.putIfAbsent(name, topic);

        if (existing != null) {
            throw new IllegalArgumentException(
                    "Topic already exists: " + name
            );
        }

        return topic;
    }

    public Topic getTopic(String name) {
        return topics.get(name);
    }

    public boolean topicExists(String name) {
        return topics.containsKey(name);
    }

    public Collection<Topic> getAllTopics() {
        return topics.values();
    }
}
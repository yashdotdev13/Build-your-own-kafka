package com.buildyourownkafka.broker;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TopicManager {

    private final Map<String, Topic> topics =
            new ConcurrentHashMap<>();

    private final Path dataDirectory;

    public TopicManager(Path dataDirectory) {

        if (dataDirectory == null) {
            throw new IllegalArgumentException(
                    "Data directory cannot be null");
        }

        this.dataDirectory = dataDirectory;
    }

    public Topic createTopic(String name, int partitionCount) {

        Topic topic =
                new Topic(
                        name,
                        partitionCount,
                        dataDirectory
                );

        Topic existing =
                topics.putIfAbsent(name, topic);

        if (existing != null) {
            throw new IllegalArgumentException(
                    "Topic already exists: " + name);
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
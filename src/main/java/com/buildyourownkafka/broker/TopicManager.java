package com.buildyourownkafka.broker;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TopicManager {

    private final Map<String, Topic> topics = new ConcurrentHashMap<>();

    private final Path dataDirectory;
    public TopicManager(Path dataDirectory) {
        if (dataDirectory == null) {
            throw new IllegalArgumentException("Data directory cannot be null");
        }
        this.dataDirectory = dataDirectory;
        recoverTopics();
    }
    public Topic createTopic(String name, int partitionCount) {
        Topic topic = new Topic(name, partitionCount, dataDirectory);
        Topic existing = topics.putIfAbsent(name, topic);
        if (existing != null) {
            throw new IllegalArgumentException("Topic already exists: " + name);
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
    private void recoverTopics() {

        if (!Files.exists(dataDirectory)) {
            return;
        }

        try (var topicDirectories = Files.list(dataDirectory)) {
            topicDirectories.filter(Files::isDirectory).forEach(this::recoverTopic);
        } catch (IOException e) {
            throw new RuntimeException("Failed to recover topics from disk", e);
        }
    }
    private void recoverTopic(Path topicDirectory) {
        String topicName = topicDirectory.getFileName().toString();

        try {
            List<Path> partitionDirectories;
            try (var directories = Files.list(topicDirectory)) {
                partitionDirectories = directories.filter(Files::isDirectory).filter(path -> path.getFileName().toString().startsWith("partition-")).sorted(Comparator.comparingInt(this::extractPartitionId)).toList();
            }
            if (partitionDirectories.isEmpty()) {
                return;
            }
            int partitionCount = partitionDirectories.size();
            Topic topic = new Topic(topicName, partitionCount, dataDirectory);
            topics.put(topicName, topic);
            System.out.println("Recovered topic: " + topicName + " with " + partitionCount + " partition(s)");
        } catch (IOException e) {
            throw new RuntimeException("Failed to recover topic: " + topicName, e);
        }
    }

    private int extractPartitionId(Path partitionDirectory) {
        String directoryName = partitionDirectory.getFileName().toString();
        String id = directoryName.substring("partition-".length());
        return Integer.parseInt(id);
    }
}
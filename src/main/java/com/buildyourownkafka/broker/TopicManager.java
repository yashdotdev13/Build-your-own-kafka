package com.buildyourownkafka.broker;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TopicManager {

    private static final Pattern PARTITION_DIRECTORY_PATTERN = Pattern.compile("partition-(0|[1-9][0-9]*)");

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

        if (!Files.isDirectory(dataDirectory)) {
            throw new IllegalStateException(
                    "Broker data path is not a directory: " + dataDirectory
            );
        }

        try (var entries = Files.list(dataDirectory)) {
            for (Path entry : entries.toList()) {
                if (!Files.isDirectory(entry)) {
                    throw new IllegalStateException(
                            "Unexpected entry in broker data directory: " + entry
                    );
                }

                recoverTopic(entry);
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to recover topics from disk: " + dataDirectory,
                    e
            );
        }
    }



    private void recoverTopic(Path topicDirectory) {
        String topicName = topicDirectory.getFileName().toString();

        try {
            List<Path> entries;

            try (var paths = Files.list(topicDirectory)) {
                entries = paths.toList();
            }

            List<Integer> partitionIds = new java.util.ArrayList<>();

            for (Path entry : entries) {
                String entryName = entry.getFileName().toString();

                Matcher matcher =
                        PARTITION_DIRECTORY_PATTERN.matcher(entryName);

                if (!matcher.matches()) {
                    throw new IllegalStateException(
                            "Unexpected entry in topic directory: " + entry
                    );
                }

                if (!Files.isDirectory(entry)) {
                    throw new IllegalStateException(
                            "Partition entry must be a directory: " + entry
                    );
                }

                try {
                    partitionIds.add(Integer.parseInt(matcher.group(1)));
                } catch (NumberFormatException e) {
                    throw new IllegalStateException(
                            "Partition ID is outside the supported integer range: "
                                    + entry,
                            e
                    );
                }
            }

            if (partitionIds.isEmpty()) {
                return;
            }

            partitionIds.sort(Comparator.naturalOrder());

            Topic topic = new Topic(topicName, partitionIds, dataDirectory);

            Topic existing = topics.putIfAbsent(topicName, topic);
            if (existing != null) {
                throw new IllegalStateException(
                        "Duplicate recovered topic: " + topicName
                );
            }

            System.out.println(
                    "Recovered topic: " + topicName
                            + " with " + partitionIds.size() + " partition(s)"
            );

        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to recover topic: " + topicName, e
            );
        }
    }


    private int extractPartitionId(Path partitionDirectory) {
        String directoryName = partitionDirectory.getFileName().toString();
        Matcher matcher = PARTITION_DIRECTORY_PATTERN.matcher(directoryName);
        if (!matcher.matches()) {
            throw new IllegalStateException("Invalid partition directory: " + partitionDirectory);
        }
        try {
            return Integer.parseInt(matcher.group(1));
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Partition ID is outside the supported integer range: "
                    + partitionDirectory, e);
        }
    }
}

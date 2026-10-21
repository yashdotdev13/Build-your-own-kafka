package com.buildyourownkafka.broker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TopicManagerRecoveryTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldRecoverExistingPartitionIdsWithoutRenumbering() throws Exception {
        Path topicDirectory = tempDir.resolve("orders");
        Files.createDirectories(topicDirectory.resolve("partition-0"));
        Files.createDirectories(topicDirectory.resolve("partition-2"));
        TopicManager topicManager = new TopicManager(tempDir);
        Topic topic = topicManager.getTopic("orders");
        assertNotNull(topic);
        assertEquals(2, topic.partitionCount());
        assertEquals(List.of(0, 2), topic.partitions().stream().map(Partition::id).toList());
        assertNotNull(topic.getPartition(2));
    }

    @Test
    void shouldRecoverContiguousPartitionIds() throws Exception {
        Path topicDirectory = tempDir.resolve("payments");
        Files.createDirectories(topicDirectory.resolve("partition-0"));
        Files.createDirectories(topicDirectory.resolve("partition-1"));
        TopicManager topicManager = new TopicManager(tempDir);
        Topic topic = topicManager.getTopic("payments");
        assertNotNull(topic);
        assertEquals(List.of(0, 1), topic.partitions().stream().map(Partition::id).toList());
    }

    @Test
    void shouldRejectMalformedPartitionDirectory() throws Exception {
        Path topicDirectory = tempDir.resolve("orders");
        Files.createDirectories(topicDirectory.resolve("partition-0"));
        Files.createDirectories(topicDirectory.resolve("partition-backup"));
        RuntimeException exception = assertThrows(RuntimeException.class, () -> new TopicManager(tempDir));
        assertTrue(exception.getMessage().contains("orders") || exception.getMessage().contains("partition"), "Expected a meaningful topic or partition recovery error");
    }

    @Test
    void shouldIgnoreTopicDirectoryWithoutPartitions() throws Exception {
        Files.createDirectories(tempDir.resolve("empty-topic"));
        TopicManager topicManager = new TopicManager(tempDir);
        assertNull(topicManager.getTopic("empty-topic"));
    }

    @Test
    void shouldRejectUnexpectedFileInTopicDirectory() throws Exception {
        Path topicDirectory = tempDir.resolve("orders");
        Files.createDirectories(topicDirectory.resolve("partition-0"));
        Files.createFile(topicDirectory.resolve("metadata.txt"));
        assertThrows(RuntimeException.class, () -> new TopicManager(tempDir));
    }

    @Test
    void shouldRejectMalformedPartitionEntryEvenWithoutValidPartitions() throws Exception {
        Path topicDirectory = tempDir.resolve("orders");
        Files.createDirectories(topicDirectory);
        Files.createFile(topicDirectory.resolve("partition-backup.txt"));
        assertThrows(RuntimeException.class, () -> new TopicManager(tempDir));
    }


    @Test
    void shouldRejectUnexpectedFileInDataDirectory() throws Exception {
        Files.createFile(tempDir.resolve("unexpected.txt"));
        assertThrows(RuntimeException.class, () -> new TopicManager(tempDir));
    }
}

package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.PartitionLog;
import com.buildyourownkafka.broker.Record;

import java.nio.file.Files;
import java.nio.file.Path;

public class SegmentRotationTest {

    public static void main(String[] args) throws Exception {

        Path directory =
                Path.of(
                        "data",
                        "segment-rotation-test"
                );

        Files.createDirectories(directory);

        try (var files = Files.list(directory)) {
            files.forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        PartitionLog log =
                new PartitionLog(directory);

        for (int i = 0; i < 9; i++) {

            log.append(
                    new Record(
                            i,
                            ("message-" + i).getBytes()
                    )
            );
        }

        System.out.println();

        System.out.println(
                "Next offset: "
                        + log.nextOffset()
        );

        System.out.println();

        System.out.println(
                "Segment files:"
        );

        try (var files = Files.list(directory)) {

            files
                    .sorted()
                    .forEach(path ->
                            System.out.println(
                                    path.getFileName()
                            )
                    );
        }

        System.out.println();

        for (int i = 0; i < 9; i++) {

            Record record =
                    log.read(i);

            System.out.println(
                    "Offset "
                            + record.offset()
                            + " -> "
                            + new String(
                            record.value()
                    )
            );
        }
    }
}
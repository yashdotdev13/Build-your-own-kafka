package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.PartitionLog;
import com.buildyourownkafka.broker.Record;

import java.nio.file.Path;

public class SegmentRecoveryTest {

    public static void main(String[] args) {

        Path directory =
                Path.of(
                        "data",
                        "segment-rotation-test"
                );

        System.out.println(
                "=== SEGMENT RECOVERY ==="
        );

        PartitionLog log =
                new PartitionLog(directory);

        System.out.println();

        System.out.println(
                "Recovered next offset: "
                        + log.nextOffset()
        );

        System.out.println();

        for (int i = 0; i < 9; i++) {

            Record record =
                    log.read(i);

            if (record == null) {

                System.out.println(
                        "Offset "
                                + i
                                + " -> NOT FOUND"
                );

                continue;
            }

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
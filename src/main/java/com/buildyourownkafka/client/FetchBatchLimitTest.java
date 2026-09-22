package com.buildyourownkafka.client;

import com.buildyourownkafka.protocol.CreateTopicPayload;
import com.buildyourownkafka.protocol.FetchPayload;
import com.buildyourownkafka.protocol.FetchResponsePayload;
import com.buildyourownkafka.protocol.ProducePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestEncoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseDecoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class FetchBatchLimitTest {

    public static void main(String[] args) throws Exception {

        try (Socket socket = new Socket("localhost", 9092)) {

            DataInputStream input =
                    new DataInputStream(socket.getInputStream());

            DataOutputStream output =
                    new DataOutputStream(socket.getOutputStream());

            RequestEncoder requestEncoder =
                    new RequestEncoder(output);

            ResponseDecoder responseDecoder =
                    new ResponseDecoder(input);

            System.out.println("Connected to broker.");

            // 1. Create topic
            CreateTopicPayload topicPayload =
                    new CreateTopicPayload("batch-test-2", 1);

            Request createTopicRequest =
                    new Request(
                            Request.CREATE_TOPIC,
                            (short) 1,
                            100,
                            topicPayload.encode()
                    );

            requestEncoder.encode(createTopicRequest);

            Response createTopicResponse =
                    responseDecoder.decode();

            if (createTopicResponse.status() != Response.SUCCESS) {

                throw new RuntimeException(
                        "CREATE_TOPIC failed: "
                                + new String(
                                createTopicResponse.payload(),
                                StandardCharsets.UTF_8
                        )
                );
            }

            System.out.println("Topic created: batch-test-2");

            // 2. Produce five records
            for (int i = 0; i < 5; i++) {

                ProducePayload payload =
                        new ProducePayload(
                                "batch-test-2",
                                0,
                                ("message-" + i)
                                        .getBytes(StandardCharsets.UTF_8)
                        );

                Request request =
                        new Request(
                                Request.PRODUCE,
                                (short) 1,
                                200 + i,
                                payload.encode()
                        );

                requestEncoder.encode(request);

                Response response =
                        responseDecoder.decode();

                if (response.status() != Response.SUCCESS) {

                    throw new RuntimeException(
                            "PRODUCE failed: "
                                    + new String(
                                    response.payload(),
                                    StandardCharsets.UTF_8
                            )
                    );
                }
            }

            System.out.println("Produced 5 records.");

            // 3. Fetch only 2 records
            List<com.buildyourownkafka.broker.Record> firstBatch =
                    fetch(
                            requestEncoder,
                            responseDecoder,
                            0,
                            2,
                            300
                    );

            if (firstBatch.size() != 2) {

                throw new AssertionError(
                        "Expected 2 records but received "
                                + firstBatch.size()
                );
            }

            if (firstBatch.get(0).offset() != 0
                    || firstBatch.get(1).offset() != 1) {

                throw new AssertionError(
                        "First batch contains incorrect offsets"
                );
            }

            System.out.println(
                    "First batch verified: offsets [0, 1]"
            );

            // 4. Fetch from offset 2
            List<com.buildyourownkafka.broker.Record> secondBatch =
                    fetch(
                            requestEncoder,
                            responseDecoder,
                            2,
                            10,
                            301
                    );

            if (secondBatch.size() != 3) {

                throw new AssertionError(
                        "Expected 3 records but received "
                                + secondBatch.size()
                );
            }

            if (secondBatch.get(0).offset() != 2
                    || secondBatch.get(1).offset() != 3
                    || secondBatch.get(2).offset() != 4) {

                throw new AssertionError(
                        "Second batch contains incorrect offsets"
                );
            }

            System.out.println(
                    "Second batch verified: offsets [2, 3, 4]"
            );

            System.out.println();
            System.out.println(
                    "FETCH BATCH LIMIT TEST PASSED!"
            );
        }
    }

    private static List<com.buildyourownkafka.broker.Record> fetch(
            RequestEncoder requestEncoder,
            ResponseDecoder responseDecoder,
            long offset,
            int maxRecords,
            int correlationId
    ) throws Exception {

        FetchPayload payload =
                new FetchPayload(
                        "batch-test-2",
                        0,
                        offset,
                        maxRecords
                );

        Request request =
                new Request(
                        Request.FETCH,
                        (short) 1,
                        correlationId,
                        payload.encode()
                );

        requestEncoder.encode(request);

        Response response =
                responseDecoder.decode();

        if (response.status() != Response.SUCCESS) {

            throw new RuntimeException(
                    "FETCH failed: "
                            + new String(
                            response.payload(),
                            StandardCharsets.UTF_8
                    )
            );
        }

        FetchResponsePayload responsePayload =
                FetchResponsePayload.decode(
                        response.payload()
                );

        List<com.buildyourownkafka.broker.Record> records =
                responsePayload.records();

        System.out.println();
        System.out.println(
                "FETCH offset=" + offset
                        + ", maxRecords=" + maxRecords
        );

        System.out.println(
                "Records returned: " + records.size()
        );

        for (var record : records) {

            System.out.println(
                    "Offset "
                            + record.offset()
                            + " -> "
                            + new String(
                            record.value(),
                            StandardCharsets.UTF_8
                    )
            );
        }

        return records;
    }
}
package com.buildyourownkafka.client;

import com.buildyourownkafka.protocol.CreateTopicPayload;
import com.buildyourownkafka.protocol.MetadataPayload;
import com.buildyourownkafka.protocol.MetadataResponsePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestEncoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseDecoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.List;

public class MetadataTest {

    public static void main(String[] args) throws Exception {

        try (Socket socket =
                     new Socket("localhost", 9092)) {

            DataInputStream input =
                    new DataInputStream(
                            socket.getInputStream()
                    );

            DataOutputStream output =
                    new DataOutputStream(
                            socket.getOutputStream()
                    );

            RequestEncoder requestEncoder =
                    new RequestEncoder(output);

            ResponseDecoder responseDecoder =
                    new ResponseDecoder(input);

            System.out.println(
                    "Connected to broker."
            );

            // Create topic
            CreateTopicPayload createPayload =
                    new CreateTopicPayload(
                            "metadata-test",
                            3
                    );

            Request createRequest =
                    new Request(
                            Request.CREATE_TOPIC,
                            (short) 1,
                            100,
                            createPayload.encode()
                    );

            requestEncoder.encode(createRequest);

            Response createResponse =
                    responseDecoder.decode();

            if (createResponse.status()
                    != Response.SUCCESS) {

                throw new RuntimeException(
                        "CREATE_TOPIC failed"
                );
            }

            System.out.println(
                    "Topic created: metadata-test"
            );

            // Metadata request
            MetadataPayload metadataPayload =
                    new MetadataPayload(
                            "metadata-test"
                    );

            Request metadataRequest =
                    new Request(
                            Request.METADATA,
                            (short) 1,
                            200,
                            metadataPayload.encode()
                    );

            requestEncoder.encode(metadataRequest);

            Response metadataResponse =
                    responseDecoder.decode();

            if (metadataResponse.status()
                    != Response.SUCCESS) {

                throw new RuntimeException(
                        "METADATA failed"
                );
            }

            MetadataResponsePayload responsePayload =
                    MetadataResponsePayload.decode(
                            metadataResponse.payload()
                    );

            System.out.println();
            System.out.println(
                    "Metadata received:"
            );

            System.out.println(
                    "Topic: "
                            + responsePayload.topicName()
            );

            System.out.println(
                    "Partition count: "
                            + responsePayload.partitionCount()
            );

            System.out.println(
                    "Partitions: "
                            + responsePayload.partitionIds()
            );

            // Assertions
            if (!responsePayload.topicName()
                    .equals("metadata-test")) {

                throw new AssertionError(
                        "Incorrect topic name"
                );
            }

            if (responsePayload.partitionCount()
                    != 3) {

                throw new AssertionError(
                        "Expected 3 partitions but received "
                                + responsePayload.partitionCount()
                );
            }

            List<Integer> expected =
                    List.of(0, 1, 2);

            if (!responsePayload.partitionIds()
                    .equals(expected)) {

                throw new AssertionError(
                        "Incorrect partition IDs: "
                                + responsePayload.partitionIds()
                );
            }

            System.out.println();
            System.out.println(
                    "METADATA TEST PASSED!"
            );
        }
    }
}
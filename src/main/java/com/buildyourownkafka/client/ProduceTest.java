package com.buildyourownkafka.client;

import com.buildyourownkafka.protocol.CreateTopicPayload;
import com.buildyourownkafka.protocol.Frame;
import com.buildyourownkafka.protocol.FrameDecoder;
import com.buildyourownkafka.protocol.FrameEncoder;
import com.buildyourownkafka.protocol.ProducePayload;
import com.buildyourownkafka.protocol.ProduceResponsePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestEncoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseDecoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class ProduceTest {

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

            FrameEncoder frameEncoder =
                    new FrameEncoder(output);

            FrameDecoder frameDecoder =
                    new FrameDecoder(input);

            RequestEncoder requestEncoder =
                    new RequestEncoder(output);

            ResponseDecoder responseDecoder =
                    new ResponseDecoder(input);

            System.out.println(
                    "Connected to broker."
            );

            // ========================================
            // CREATE TOPIC
            // ========================================

            CreateTopicPayload topicPayload =
                    new CreateTopicPayload(
                            "orders",
                            3
                    );

            Request createTopicRequest =
                    new Request(
                            Request.CREATE_TOPIC,
                            (short) 1,
                            100,
                            topicPayload.encode()
                    );

            requestEncoder.encode(
                    createTopicRequest
            );

            Response createTopicResponse =
                    responseDecoder.decode();

            System.out.println();
            System.out.println(
                    "CREATE_TOPIC RESPONSE"
            );

            System.out.println(
                    "Correlation ID: " +
                            createTopicResponse.correlationId()
            );

            System.out.println(
                    "Status: " +
                            createTopicResponse.status()
            );

            System.out.println(
                    "Payload: " +
                            new String(
                                    createTopicResponse.payload(),
                                    StandardCharsets.UTF_8
                            )
            );

            // ========================================
            // PRODUCE #1
            // ========================================

            produce(
                    requestEncoder,
                    responseDecoder,
                    "orders",
                    1,
                    "order-123",
                    101
            );

            // ========================================
            // PRODUCE #2
            // ========================================

            produce(
                    requestEncoder,
                    responseDecoder,
                    "orders",
                    1,
                    "order-456",
                    102
            );

            // ========================================
            // PRODUCE #3
            // ========================================

            produce(
                    requestEncoder,
                    responseDecoder,
                    "orders",
                    1,
                    "order-789",
                    103
            );
        }
    }

    private static void produce(
            RequestEncoder requestEncoder,
            ResponseDecoder responseDecoder,
            String topic,
            int partition,
            String value,
            int correlationId
    ) throws Exception {

        ProducePayload payload =
                new ProducePayload(
                        topic,
                        partition,
                        value.getBytes(
                                StandardCharsets.UTF_8
                        )
                );

        Request request =
                new Request(
                        Request.PRODUCE,
                        (short) 1,
                        correlationId,
                        payload.encode()
                );

        // Send request
        requestEncoder.encode(request);

        // Receive response
        Response response =
                responseDecoder.decode();

        System.out.println();
        System.out.println(
                "PRODUCE RESPONSE"
        );

        System.out.println(
                "Correlation ID: " +
                        response.correlationId()
        );

        System.out.println(
                "Status: " +
                        response.status()
        );

        if (response.status() == Response.SUCCESS) {

            ProduceResponsePayload responsePayload =
                    ProduceResponsePayload.decode(
                            response.payload()
                    );

            System.out.println(
                    "Topic: " + topic
            );

            System.out.println(
                    "Partition: " + partition
            );

            System.out.println(
                    "Value: " + value
            );

            System.out.println(
                    "Offset: " +
                            responsePayload.offset()
            );

        } else {

            System.out.println(
                    "Error: " +
                            new String(
                                    response.payload(),
                                    StandardCharsets.UTF_8
                            )
            );
        }
    }
}
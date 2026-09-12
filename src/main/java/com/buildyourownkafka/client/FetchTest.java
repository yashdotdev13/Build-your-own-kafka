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

public class FetchTest {

    public static void main(String[] args) throws Exception {

        try (Socket socket = new Socket("localhost", 9092)) {
            DataInputStream input = new DataInputStream(socket.getInputStream());
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            RequestEncoder requestEncoder = new RequestEncoder(output);
            ResponseDecoder responseDecoder = new ResponseDecoder(input);
            System.out.println("Connected to broker.");


            CreateTopicPayload topicPayload = new CreateTopicPayload("orders", 3);
            Request createTopicRequest = new Request(Request.CREATE_TOPIC, (short) 1, 100, topicPayload.encode());
            requestEncoder.encode(createTopicRequest);
            Response createTopicResponse = responseDecoder.decode();

            System.out.println();
            System.out.println("CREATE_TOPIC RESPONSE");

            System.out.println("Status: " + createTopicResponse.status());

            produce(requestEncoder, responseDecoder, "orders", 1, "order-123", 101);
            produce(requestEncoder, responseDecoder, "orders", 1, "order-456", 102);
            produce(requestEncoder, responseDecoder, "orders", 1, "order-789", 103);

            fetch(requestEncoder, responseDecoder, "orders", 1, 0, 200);
            fetch(requestEncoder, responseDecoder, "orders", 1, 1, 201);

            fetch(requestEncoder, responseDecoder, "orders", 1, 2, 202);
            fetch(requestEncoder, responseDecoder, "orders", 1, 3, 203);
        }
    }

    private static void produce(RequestEncoder requestEncoder, ResponseDecoder responseDecoder, String topic, int partition, String value, int correlationId) throws Exception {

        ProducePayload payload = new ProducePayload(topic, partition, value.getBytes(StandardCharsets.UTF_8));
        Request request = new Request(Request.PRODUCE, (short) 1, correlationId, payload.encode());
        requestEncoder.encode(request);
        Response response = responseDecoder.decode();

        if (response.status() != Response.SUCCESS) {
            System.out.println("PRODUCE failed: " + new String(response.payload(), StandardCharsets.UTF_8));
            return;
        }

        System.out.println();
        System.out.println("PRODUCE");
        System.out.println("Value: " + value);
        System.out.println("Correlation ID: " + response.correlationId());
        System.out.println("Status: " + response.status());
    }

    private static void fetch(RequestEncoder requestEncoder, ResponseDecoder responseDecoder, String topic, int partition, long offset, int correlationId) throws Exception {

        FetchPayload payload = new FetchPayload(topic, partition, offset);
        Request request = new Request(Request.FETCH, (short) 1, correlationId, payload.encode());
        requestEncoder.encode(request);
        Response response = responseDecoder.decode();

        System.out.println();
        System.out.println("FETCH");
        System.out.println("Requested offset: " + offset);
        System.out.println("Correlation ID: " + response.correlationId());
        System.out.println("Status: " + response.status());

        if (response.status() != Response.SUCCESS) {
            System.out.println("Error: " + new String(response.payload(), StandardCharsets.UTF_8));
            return;
        }
        FetchResponsePayload responsePayload = FetchResponsePayload.decode(response.payload());
        System.out.println("Records returned: " + responsePayload.records().size());
        for (com.buildyourownkafka.broker.Record record : responsePayload.records()) {
            System.out.println("Offset " + record.offset() + " -> " + new String(record.value(), StandardCharsets.UTF_8));
        }
    }
}
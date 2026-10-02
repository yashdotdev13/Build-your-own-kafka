package com.buildyourownkafka.client;

import com.buildyourownkafka.protocol.CreateTopicPayload;
import com.buildyourownkafka.protocol.FetchPayload;
import com.buildyourownkafka.protocol.FetchResponsePayload;
import com.buildyourownkafka.protocol.ProduceBatchPayload;
import com.buildyourownkafka.protocol.ProduceBatchResponsePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestEncoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseDecoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ProduceBatchTest {

    public static void main(String[] args) throws Exception {

        String topic = "batch-orders";
        int partition = 1;

        try (Socket socket = new Socket("localhost", 9092)) {

            DataInputStream input = new DataInputStream(socket.getInputStream());
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            RequestEncoder requestEncoder = new RequestEncoder(output);
            ResponseDecoder responseDecoder = new ResponseDecoder(input);

            // Create topic
            CreateTopicPayload createPayload = new CreateTopicPayload(topic, 3);
            Request createRequest = new Request(Request.CREATE_TOPIC, (short) 1, 200, createPayload.encode());
            requestEncoder.encode(createRequest);
            Response createResponse = responseDecoder.decode();
            if (createResponse.status() != Response.SUCCESS) {
                throw new RuntimeException("Topic creation failed: " + new String(createResponse.payload(), StandardCharsets.UTF_8));
            }
            System.out.println("Topic created successfully.");
            // Prepare batch
            List<byte[]> records = List.of("order-101".getBytes(StandardCharsets.UTF_8), "order-102".getBytes(StandardCharsets.UTF_8), "order-103"
                    .getBytes(StandardCharsets.UTF_8));
            ProduceBatchPayload batchPayload = new ProduceBatchPayload(topic, partition, records);
            Request produceRequest = new Request(Request.PRODUCE_BATCH, (short) 1, 201, batchPayload.encode());

            // Send exactly one PRODUCE_BATCH request
            requestEncoder.encode(produceRequest);
            Response produceResponse = responseDecoder.decode();
            if (produceResponse.status() != Response.SUCCESS) {
                throw new RuntimeException("Batch produce failed: " + new String(produceResponse.payload(),
                        StandardCharsets.UTF_8));
            }
            ProduceBatchResponsePayload batchResponse = ProduceBatchResponsePayload.decode(produceResponse.payload());

            System.out.println();
            System.out.println("BATCH PRODUCE RESPONSE");
            System.out.println("Base offset: " + batchResponse.baseOffset());
            System.out.println("Record count: " + batchResponse.recordCount());

            // Fetch the records
            FetchPayload fetchPayload = new FetchPayload(topic, partition, batchResponse.baseOffset(),
                    batchResponse.recordCount());

            Request fetchRequest = new Request(Request.FETCH, (short) 1, 202, fetchPayload.encode());
            requestEncoder.encode(fetchRequest);
            Response fetchResponse = responseDecoder.decode();
            if (fetchResponse.status() != Response.SUCCESS) {
                throw new RuntimeException("Fetch failed: " + new String(fetchResponse.payload(), StandardCharsets.UTF_8));
            }
            FetchResponsePayload fetched = FetchResponsePayload.decode(fetchResponse.payload());
            System.out.println();
            System.out.println("FETCHED RECORDS");

            for (var record : fetched.records()) {
                System.out.println("Offset: " + record.offset() + ", Value: " + new String(record.value(), StandardCharsets.UTF_8));
            }
            if (fetched.records().size() != records.size()) {
                throw new AssertionError("Expected " + records.size() + " records, got " + fetched.records().size());
            }
            for (int i = 0; i < records.size(); i++) {
                var actual = fetched.records().get(i);

                String expectedValue = new String(records.get(i), StandardCharsets.UTF_8);
                String actualValue = new String(actual.value(), StandardCharsets.UTF_8);
                if (actual.offset() != batchResponse.baseOffset() + i || !actualValue.equals(expectedValue)) {
                    throw new AssertionError("Batch record mismatch at index " + i);
                }
            }
            System.out.println();
            System.out.println("Batch integration test passed.");
        }
    }

}
package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.Record;
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
import java.util.List;

public class ConsumerCommitTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== CONSUMER COMMIT / RECOVERY TEST ===");
        String host = "localhost";
        int port = 9092;
        String topic = "consumer-recovery-test";
        int partition = 0;
        String groupId = "payment-service";

        System.out.println();
        System.out.println("--- PRODUCING TEST DATA ---");
        produce(host, port, topic, partition, "payment-1");
        produce(host, port, topic, partition, "payment-2");
        produce(host, port, topic, partition, "payment-3");
        System.out.println("Test records produced.");

        System.out.println();
        System.out.println("--- CONSUMER #1 ---");
        Consumer consumer1 = new Consumer(host, port, topic, partition, 0L, groupId);
        System.out.println("Starting offset: " + consumer1.currentOffset());
        List<Record> records = consumer1.poll();
        System.out.println("Records received: " + records.size());
        for (Record record : records) {
            System.out.println("Offset " + record.offset() + " -> " + new String(record.value(), StandardCharsets.UTF_8));
        }
        System.out.println("Current offset after poll: " + consumer1.currentOffset());
        if (records.isEmpty()) {
            throw new RuntimeException("Consumer did not receive any records");
        }
        consumer1.commit();
        long committedOffset = consumer1.currentOffset();
        System.out.println("Committed offset: " + committedOffset);
        consumer1.close();
        System.out.println();
        System.out.println("--- CONSUMER RESTART ---");
        Consumer consumer2 = new Consumer(host, port, topic, partition, groupId);
        long recoveredOffset = consumer2.currentOffset();
        System.out.println("Recovered offset: " + recoveredOffset);
        if (recoveredOffset != committedOffset) {
            throw new RuntimeException("Offset recovery failed. Expected " + committedOffset + " but got " + recoveredOffset);
        }
        System.out.println();
        System.out.println("--- POLL AFTER RESTART ---");
        List<Record> recoveredRecords = consumer2.poll();
        System.out.println("Records received after restart: " + recoveredRecords.size());
        if (!recoveredRecords.isEmpty()) {
            throw new RuntimeException("Consumer received already committed records");
        }
        System.out.println("Current offset after restart poll: " + consumer2.currentOffset());
        consumer2.close();
        System.out.println();
        System.out.println("========================================");
        System.out.println("Consumer offset recovery verified!");
        System.out.println("Committed offset: " + committedOffset);
        System.out.println("Recovered offset: " + recoveredOffset);
        System.out.println("Already committed records were not replayed.");
        System.out.println("========================================");
    }
    private static void produce(String host, int port, String topic, int partition, String value) throws Exception {

        try (Socket socket = new Socket(host, port)) {
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            DataInputStream input = new DataInputStream(socket.getInputStream());
            RequestEncoder requestEncoder = new RequestEncoder(output);
            ResponseDecoder responseDecoder = new ResponseDecoder(input);
            ProducePayload payload = new ProducePayload(topic, partition, value.getBytes(StandardCharsets.UTF_8));
            Request request = new Request(Request.PRODUCE, (short) 1, 1, payload.encode());
            requestEncoder.encode(request);
            Response response = responseDecoder.decode();
            if (response.status() != Response.SUCCESS) {
                throw new RuntimeException("Failed to produce test record: " + new String(response.payload()));
            }
            ProduceResponsePayload responsePayload = ProduceResponsePayload.decode(response.payload());
            System.out.println("Produced record at offset: " + responsePayload.offset());
        }
    }
}
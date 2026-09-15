package com.buildyourownkafka.client;

import com.buildyourownkafka.protocol.FetchPayload;
import com.buildyourownkafka.protocol.FetchResponsePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestEncoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseDecoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RecoveryFetchTest {

    public static void main(String[] args) throws Exception {

        try (Socket socket = new Socket("localhost", 9092)) {

            DataInputStream input = new DataInputStream(socket.getInputStream());
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            RequestEncoder requestEncoder = new RequestEncoder(output);
            ResponseDecoder responseDecoder = new ResponseDecoder(input);
            System.out.println("Connected to broker.");
            FetchPayload payload = new FetchPayload("orders", 1, 0);
            Request request = new Request(Request.FETCH, (short) 1, 400, payload.encode());
            requestEncoder.encode(request);
            Response response = responseDecoder.decode();
            System.out.println();
            System.out.println("FETCH AFTER RESTART");
            System.out.println("Correlation ID: " + response.correlationId());
            System.out.println("Status: " + response.status());

            if (response.status() == Response.SUCCESS) {
                FetchResponsePayload fetchResponse = FetchResponsePayload.decode(response.payload());
                List<com.buildyourownkafka.broker.Record> records = fetchResponse.records();
                System.out.println("Records fetched: " + records.size());
                System.out.println();
                for (var record : records) {
                    System.out.println("Offset " + record.offset() + " -> " + new String(record.value(), StandardCharsets.UTF_8));
                }
            } else {
                System.out.println("Error: " + new String(response.payload(), StandardCharsets.UTF_8));
            }
        }
    }
}
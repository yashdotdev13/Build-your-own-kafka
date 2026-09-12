package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.Record;
import com.buildyourownkafka.protocol.FetchResponsePayload;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class FetchResponsePayloadTest {

    public static void main(String[] args) {

        List<Record> records = List.of(new Record(0, "order-123".getBytes(StandardCharsets.UTF_8)),
                new Record(1, "order-456".getBytes(StandardCharsets.UTF_8)),
                new Record(2, "order-789".getBytes(StandardCharsets.UTF_8)));
        FetchResponsePayload original = new FetchResponsePayload(records);

        byte[] encoded = original.encode();
        System.out.println("Encoded payload size: " + encoded.length);
        FetchResponsePayload decoded = FetchResponsePayload.decode(encoded);
        System.out.println("Record count: " + decoded.records().size());

        for (Record record : decoded.records()) {
            System.out.println("Offset " + record.offset() + " -> " + new String(record.value(), StandardCharsets.UTF_8));
        }
    }
}
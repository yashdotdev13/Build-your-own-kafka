package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.Record;
import com.buildyourownkafka.protocol.FetchPayload;
import com.buildyourownkafka.protocol.FetchResponsePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestEncoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseDecoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.List;

public class Consumer implements AutoCloseable {

    private final String topic;
    private final int partition;

    private long currentOffset;

    private final Socket socket;

    private final DataInputStream input;
    private final DataOutputStream output;

    private final RequestEncoder requestEncoder;
    private final ResponseDecoder responseDecoder;

    private int correlationId = 1000;

    public Consumer(
            String host,
            int port,
            String topic,
            int partition,
            long startingOffset) throws Exception {

        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException(
                    "Host cannot be blank");
        }

        if (port <= 0) {
            throw new IllegalArgumentException(
                    "Port must be greater than zero");
        }

        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException(
                    "Topic cannot be blank");
        }

        if (partition < 0) {
            throw new IllegalArgumentException(
                    "Partition cannot be negative");
        }

        if (startingOffset < 0) {
            throw new IllegalArgumentException(
                    "Starting offset cannot be negative");
        }

        this.topic = topic;
        this.partition = partition;
        this.currentOffset = startingOffset;

        this.socket =
                new Socket(host, port);

        this.input =
                new DataInputStream(
                        socket.getInputStream()
                );

        this.output =
                new DataOutputStream(
                        socket.getOutputStream()
                );

        this.requestEncoder =
                new RequestEncoder(output);

        this.responseDecoder =
                new ResponseDecoder(input);
    }

    public synchronized List<Record> poll()
            throws Exception {

        FetchPayload payload =
                new FetchPayload(
                        topic,
                        partition,
                        currentOffset
                );

        Request request =
                new Request(
                        Request.FETCH,
                        (short) 1,
                        correlationId++,
                        payload.encode()
                );

        requestEncoder.encode(request);

        Response response =
                responseDecoder.decode();

        if (response.status() != Response.SUCCESS) {

            throw new RuntimeException(
                    "Fetch failed: "
                            + new String(
                            response.payload()
                    )
            );
        }

        FetchResponsePayload fetchResponse =
                FetchResponsePayload.decode(
                        response.payload()
                );

        List<Record> records =
                fetchResponse.records();

        if (!records.isEmpty()) {

            Record lastRecord =
                    records.get(
                            records.size() - 1
                    );

            currentOffset =
                    lastRecord.offset() + 1;
        }

        return records;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public synchronized long currentOffset() {
        return currentOffset;
    }

    public synchronized void advanceOffset(
            long nextOffset) {

        if (nextOffset < currentOffset) {

            throw new IllegalArgumentException(
                    "Consumer offset cannot move backwards"
            );
        }

        this.currentOffset = nextOffset;
    }

    @Override
    public void close() throws Exception {
        socket.close();
    }
}
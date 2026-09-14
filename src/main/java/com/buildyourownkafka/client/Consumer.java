package com.buildyourownkafka.client;

import com.buildyourownkafka.broker.Record;
import com.buildyourownkafka.protocol.CommitOffsetPayload;
import com.buildyourownkafka.protocol.FetchOffsetPayload;
import com.buildyourownkafka.protocol.FetchOffsetResponsePayload;
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

    private final String consumerGroupId;

    private long currentOffset;

    private final Socket socket;

    private final DataInputStream input;
    private final DataOutputStream output;

    private final RequestEncoder requestEncoder;
    private final ResponseDecoder responseDecoder;

    private int correlationId = 1000;

    /*
     * ---------------------------------------------------------
     * Constructor 1
     * ---------------------------------------------------------
     *
     * Existing behavior:
     *
     * Consumer starts from explicitly supplied offset.
     *
     * No consumer group is used.
     */
    public Consumer(
            String host,
            int port,
            String topic,
            int partition,
            long startingOffset
    ) throws Exception {

        this(
                host,
                port,
                topic,
                partition,
                startingOffset,
                null
        );
    }

    /*
     * ---------------------------------------------------------
     * Constructor 2
     * ---------------------------------------------------------
     *
     * Consumer starts from explicitly supplied offset
     * and belongs to a consumer group.
     */
    public Consumer(
            String host,
            int port,
            String topic,
            int partition,
            long startingOffset,
            String consumerGroupId
    ) throws Exception {

        validateHost(host);
        validatePort(port);
        validateTopic(topic);
        validatePartition(partition);
        validateOffset(startingOffset);

        if (consumerGroupId != null
                && consumerGroupId.isBlank()) {

            throw new IllegalArgumentException(
                    "Consumer group ID cannot be blank"
            );
        }

        this.topic = topic;
        this.partition = partition;
        this.currentOffset = startingOffset;
        this.consumerGroupId = consumerGroupId;

        this.socket =
                new Socket(
                        host,
                        port
                );

        this.input =
                new DataInputStream(
                        socket.getInputStream()
                );

        this.output =
                new DataOutputStream(
                        socket.getOutputStream()
                );

        this.requestEncoder =
                new RequestEncoder(
                        output
                );

        this.responseDecoder =
                new ResponseDecoder(
                        input
                );
    }

    /*
     * ---------------------------------------------------------
     * Constructor 3
     * ---------------------------------------------------------
     *
     * Consumer starts from the committed offset stored
     * on the broker.
     *
     * This is the constructor we will use for
     * restart/recovery behavior.
     */
    public Consumer(
            String host,
            int port,
            String topic,
            int partition,
            String consumerGroupId
    ) throws Exception {

        this(
                host,
                port,
                topic,
                partition,
                0L,
                consumerGroupId
        );

        /*
         * Replace the initial offset with the
         * committed offset stored on the broker.
         */
        this.currentOffset =
                fetchCommittedOffset();
    }

    /*
     * ---------------------------------------------------------
     * POLL
     * ---------------------------------------------------------
     *
     * Fetch records starting from currentOffset.
     *
     * Example:
     *
     * currentOffset = 4
     *
     *      ↓
     *
     * FETCH(offset=4)
     *
     *      ↓
     *
     * records 4,5,6
     *
     *      ↓
     *
     * currentOffset = 7
     *
     * Note:
     * Poll does NOT commit the offset.
     */
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

        requestEncoder.encode(
                request
        );

        Response response =
                responseDecoder.decode();

        if (response.status()
                != Response.SUCCESS) {

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

        /*
         * Advance the local offset only when
         * records were actually received.
         */
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

    /*
     * ---------------------------------------------------------
     * COMMIT
     * ---------------------------------------------------------
     *
     * Persist the current consumer offset on the broker.
     *
     * Example:
     *
     * currentOffset = 7
     *
     *      ↓
     *
     * COMMIT_OFFSET
     *
     *      ↓
     *
     * ConsumerOffsetStore
     *
     *      ↓
     *
     * offsets.log
     */
    public synchronized void commit()
            throws Exception {

        if (consumerGroupId == null) {

            throw new IllegalStateException(
                    "Consumer group ID is required "
                            + "to commit offsets"
            );
        }

        CommitOffsetPayload payload =
                new CommitOffsetPayload(
                        consumerGroupId,
                        topic,
                        partition,
                        currentOffset
                );

        Request request =
                new Request(
                        Request.COMMIT_OFFSET,
                        (short) 1,
                        correlationId++,
                        payload.encode()
                );

        requestEncoder.encode(
                request
        );

        Response response =
                responseDecoder.decode();

        if (response.status()
                != Response.SUCCESS) {

            throw new RuntimeException(
                    "Offset commit failed: "
                            + new String(
                            response.payload()
                    )
            );
        }
    }

    /*
     * ---------------------------------------------------------
     * FETCH COMMITTED OFFSET
     * ---------------------------------------------------------
     *
     * Ask the broker for the last committed offset
     * of this consumer group.
     */
    private synchronized long fetchCommittedOffset()
            throws Exception {

        if (consumerGroupId == null) {

            throw new IllegalStateException(
                    "Consumer group ID is required "
                            + "to fetch committed offset"
            );
        }

        FetchOffsetPayload payload =
                new FetchOffsetPayload(
                        consumerGroupId,
                        topic,
                        partition
                );

        Request request =
                new Request(
                        Request.FETCH_OFFSET,
                        (short) 1,
                        correlationId++,
                        payload.encode()
                );

        requestEncoder.encode(
                request
        );

        Response response =
                responseDecoder.decode();

        if (response.status()
                != Response.SUCCESS) {

            throw new RuntimeException(
                    "Failed to fetch committed offset: "
                            + new String(
                            response.payload()
                    )
            );
        }

        FetchOffsetResponsePayload responsePayload =
                FetchOffsetResponsePayload.decode(
                        response.payload()
                );

        return responsePayload.offset();
    }

    /*
     * ---------------------------------------------------------
     * ACCESSORS
     * ---------------------------------------------------------
     */

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public String consumerGroupId() {
        return consumerGroupId;
    }

    public synchronized long currentOffset() {
        return currentOffset;
    }

    /*
     * ---------------------------------------------------------
     * MANUAL OFFSET ADVANCEMENT
     * ---------------------------------------------------------
     */

    public synchronized void advanceOffset(
            long nextOffset
    ) {

        if (nextOffset < currentOffset) {

            throw new IllegalArgumentException(
                    "Consumer offset cannot move backwards"
            );
        }

        this.currentOffset =
                nextOffset;
    }

    /*
     * ---------------------------------------------------------
     * CLOSE
     * ---------------------------------------------------------
     */

    @Override
    public void close()
            throws Exception {

        socket.close();
    }

    /*
     * ---------------------------------------------------------
     * VALIDATION
     * ---------------------------------------------------------
     */

    private static void validateHost(
            String host
    ) {

        if (host == null
                || host.isBlank()) {

            throw new IllegalArgumentException(
                    "Host cannot be blank"
            );
        }
    }

    private static void validatePort(
            int port
    ) {

        if (port <= 0) {

            throw new IllegalArgumentException(
                    "Port must be greater than zero"
            );
        }
    }

    private static void validateTopic(
            String topic
    ) {

        if (topic == null
                || topic.isBlank()) {

            throw new IllegalArgumentException(
                    "Topic cannot be blank"
            );
        }
    }

    private static void validatePartition(
            int partition
    ) {

        if (partition < 0) {

            throw new IllegalArgumentException(
                    "Partition cannot be negative"
            );
        }
    }

    private static void validateOffset(
            long offset
    ) {

        if (offset < 0) {

            throw new IllegalArgumentException(
                    "Starting offset cannot be negative"
            );
        }
    }
}
package com.buildyourownkafka.client;

import com.buildyourownkafka.protocol.CommitOffsetPayload;
import com.buildyourownkafka.protocol.FetchOffsetPayload;
import com.buildyourownkafka.protocol.FetchOffsetResponsePayload;
import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestEncoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseDecoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class OffsetHandlerTest {

    public static void main(String[] args) throws Exception {

        System.out.println("=== OFFSET HANDLER TEST ===");
        try (Socket socket = new Socket("localhost", 9092)) {

            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            DataInputStream input = new DataInputStream(socket.getInputStream());
            RequestEncoder requestEncoder = new RequestEncoder(output);
            ResponseDecoder responseDecoder = new ResponseDecoder(input);
            String groupId = "payment-service";
            String topic = "orders";
            int partition = 1;
            long committedOffset = 7;

            System.out.println();
            System.out.println("--- COMMIT OFFSET ---");
            CommitOffsetPayload commitPayload = new CommitOffsetPayload(groupId, topic, partition, committedOffset);
            Request commitRequest = new Request(Request.COMMIT_OFFSET, (short) 1, 1, commitPayload.encode());
            requestEncoder.encode(commitRequest);
            Response commitResponse = responseDecoder.decode();
            System.out.println("Commit status: " + commitResponse.status());
            if (commitResponse.status() != Response.SUCCESS) {
                throw new RuntimeException("Offset commit failed: " + new String(commitResponse.payload()));
            }
            System.out.println("Committed offset: " + committedOffset);
            System.out.println();
            System.out.println("--- FETCH OFFSET ---");
            FetchOffsetPayload fetchPayload = new FetchOffsetPayload(groupId, topic, partition);
            Request fetchRequest = new Request(Request.FETCH_OFFSET, (short) 1, 2, fetchPayload.encode());
            requestEncoder.encode(fetchRequest);
            Response fetchResponse = responseDecoder.decode();
            System.out.println("Fetch status: " + fetchResponse.status());
            if (fetchResponse.status() != Response.SUCCESS) {
                throw new RuntimeException("Offset fetch failed: " + new String(fetchResponse.payload()));
            }
            FetchOffsetResponsePayload responsePayload = FetchOffsetResponsePayload.decode(fetchResponse.payload());
            long fetchedOffset = responsePayload.offset();
            System.out.println("Fetched offset: " + fetchedOffset);
            if (fetchedOffset != committedOffset) {
                throw new RuntimeException("Offset mismatch. Expected " + committedOffset + " but got " + fetchedOffset);
            }
            System.out.println();
            System.out.println("Offset commit/fetch verified successfully!");
        }
    }
}
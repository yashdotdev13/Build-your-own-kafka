package com.buildyourownkafka.client;

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

public class RecoveryProduceTest {

    public static void main(String[] args) throws Exception {

        try (Socket socket = new Socket("localhost", 9092)) {

            DataInputStream input =
                    new DataInputStream(socket.getInputStream());

            DataOutputStream output =
                    new DataOutputStream(socket.getOutputStream());

            RequestEncoder requestEncoder =
                    new RequestEncoder(output);

            ResponseDecoder responseDecoder =
                    new ResponseDecoder(input);

            System.out.println("Connected to broker.");

            ProducePayload payload =
                    new ProducePayload(
                            "orders",
                            1,
                            "order-after-restart"
                                    .getBytes(StandardCharsets.UTF_8)
                    );

            Request request =
                    new Request(
                            Request.PRODUCE,
                            (short) 1,
                            300,
                            payload.encode()
                    );

            requestEncoder.encode(request);

            Response response =
                    responseDecoder.decode();

            System.out.println();
            System.out.println("PRODUCE AFTER RESTART");
            System.out.println("Correlation ID: "
                    + response.correlationId());
            System.out.println("Status: "
                    + response.status());

            if (response.status() == Response.SUCCESS) {

                ProduceResponsePayload produceResponse =
                        ProduceResponsePayload.decode(
                                response.payload()
                        );

                System.out.println(
                        "Recovered next offset → "
                                + produceResponse.offset()
                );
            } else {

                System.out.println(
                        "Error: "
                                + new String(
                                response.payload(),
                                StandardCharsets.UTF_8
                        )
                );
            }
        }
    }
}
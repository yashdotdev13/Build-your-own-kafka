package com.buildyourownkafka.client;

import com.buildyourownkafka.protocol.Request;
import com.buildyourownkafka.protocol.RequestEncoder;
import com.buildyourownkafka.protocol.Response;
import com.buildyourownkafka.protocol.ResponseDecoder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class TestClient {

    public static void main(String[] args)
            throws Exception {

        try (Socket socket =
                     new Socket("localhost", 9092)) {

            System.out.println(
                    "Connected to broker."
            );

            DataOutputStream output =
                    new DataOutputStream(
                            socket.getOutputStream()
                    );

            DataInputStream input =
                    new DataInputStream(
                            socket.getInputStream()
                    );

            RequestEncoder requestEncoder =
                    new RequestEncoder(output);

            ResponseDecoder responseDecoder =
                    new ResponseDecoder(input);

            Request request =
                    new Request(
                            Request.PING,
                            (short) 1,
                            42,
                            new byte[0]
                    );

            requestEncoder.encode(request);

            System.out.println(
                    "Sent request: PING"
            );

            Response response =
                    responseDecoder.decode();

            if (response == null) {
                System.out.println(
                        "Broker closed the connection."
                );
                return;
            }

            System.out.println(
                    "Received response:"
            );

            System.out.println(
                    "Correlation ID: "
                            + response.correlationId()
            );

            System.out.println(
                    "Status: "
                            + response.status()
            );

            System.out.println(
                    "Payload: "
                            + new String(
                            response.payload(),
                            StandardCharsets.UTF_8
                    )
            );
        }
    }
}
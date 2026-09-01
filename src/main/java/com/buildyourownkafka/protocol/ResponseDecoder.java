package com.buildyourownkafka.protocol;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class ResponseDecoder {

    private final FrameDecoder frameDecoder;

    public ResponseDecoder(DataInputStream input) {
        this.frameDecoder =
                new FrameDecoder(input);
    }

    public Response decode() throws IOException {

        Frame frame =
                frameDecoder.decode();

        if (frame == null) {
            return null;
        }

        DataInputStream data =
                new DataInputStream(
                        new ByteArrayInputStream(
                                frame.payload()
                        )
                );

        int correlationId = data.readInt();

        int status = data.readInt();

        int payloadLength = data.readInt();

        if (payloadLength < 0 ||
                payloadLength > frame.length()) {

            throw new IOException(
                    "Invalid response payload length: "
                            + payloadLength
            );
        }

        byte[] payload =
                new byte[payloadLength];

        data.readFully(payload);

        return new Response(
                correlationId,
                status,
                payload
        );
    }
}
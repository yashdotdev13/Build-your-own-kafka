package com.buildyourownkafka.protocol;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class RequestDecoder {

    private final FrameDecoder frameDecoder;

    public RequestDecoder(DataInputStream input) {
        this.frameDecoder =
                new FrameDecoder(input);
    }

    public Request decode() throws IOException {

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

        int type = data.readInt();

        short version = data.readShort();

        int correlationId = data.readInt();

        int payloadLength = data.readInt();

        if (payloadLength < 0 ||
                payloadLength >
                        frame.length()) {

            throw new IOException(
                    "Invalid request payload length: "
                            + payloadLength
            );
        }

        byte[] payload =
                new byte[payloadLength];

        data.readFully(payload);

        return new Request(
                type,
                version,
                correlationId,
                payload
        );
    }
}
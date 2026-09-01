package com.buildyourownkafka.protocol;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class RequestDecoder {

    private final FrameDecoder frameDecoder = new FrameDecoder();
    public Request decode(InputStream inputStream) throws IOException {

        Frame frame = frameDecoder.decode(inputStream);
        if (frame == null) {
            return null;
        }

        DataInputStream data = new DataInputStream(new ByteArrayInputStream(frame.payload()));

        int type = data.readInt();
        short version = data.readShort();
        int correlationId = data.readInt();

        int payloadLength = data.readInt();

        if (payloadLength < 0 ||
                payloadLength > frame.length()) {

            throw new IOException("Invalid request payload length: "+ payloadLength
            );
        }

        byte[] payload = new byte[payloadLength];
        data.readFully(payload);

        return new Request(
                type,
                version,
                correlationId,
                payload
        );
    }
}
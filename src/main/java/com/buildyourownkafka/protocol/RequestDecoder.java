
package com.buildyourownkafka.protocol;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class RequestDecoder {

    private static final int HEADER_SIZE = Integer.BYTES
            + Short.BYTES
            + Integer.BYTES
            + Integer.BYTES;

    private final FrameDecoder frameDecoder;

    public RequestDecoder(DataInputStream input) {
        this.frameDecoder = new FrameDecoder(input);
    }

    public Request decode() throws IOException {
        Frame frame = frameDecoder.decode();

        if (frame == null) {
            return null;
        }

        if (frame.length() < HEADER_SIZE) {
            throw new IOException("Incomplete request header");
        }

        DataInputStream data = new DataInputStream(
                new ByteArrayInputStream(frame.payload()));

        int type = data.readInt();
        short version = data.readShort();
        int correlationId = data.readInt();
        int payloadLength = data.readInt();

        int remaining = data.available();

        if (payloadLength < 0 || payloadLength != remaining) {
            throw new IOException(
                    "Invalid request payload length: declared "
                            + payloadLength + ", remaining " + remaining);
        }

        byte[] payload = new byte[payloadLength];
        data.readFully(payload);

        return new Request(type, version, correlationId, payload);
    }
}

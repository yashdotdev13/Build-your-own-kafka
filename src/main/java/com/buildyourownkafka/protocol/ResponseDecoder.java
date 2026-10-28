
package com.buildyourownkafka.protocol;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class ResponseDecoder {

    private static final int HEADER_SIZE = Integer.BYTES * 3;

    private final FrameDecoder frameDecoder;

    public ResponseDecoder(DataInputStream input) {
        this.frameDecoder = new FrameDecoder(input);
    }

    public Response decode() throws IOException {
        Frame frame = frameDecoder.decode();

        if (frame == null) {
            return null;
        }

        if (frame.length() < HEADER_SIZE) {
            throw new IOException("Incomplete response header");
        }

        DataInputStream data = new DataInputStream(
                new ByteArrayInputStream(frame.payload()));

        int correlationId = data.readInt();
        int status = data.readInt();
        int payloadLength = data.readInt();

        int remaining = data.available();

        if (payloadLength < 0 || payloadLength != remaining) {
            throw new IOException(
                    "Invalid response payload length: declared "
                            + payloadLength + ", remaining " + remaining);
        }

        byte[] payload = new byte[payloadLength];
        data.readFully(payload);

        return new Response(correlationId, status, payload);
    }
}

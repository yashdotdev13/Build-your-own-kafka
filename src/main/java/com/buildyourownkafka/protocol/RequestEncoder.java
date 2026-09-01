package com.buildyourownkafka.protocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class RequestEncoder {

    private final FrameEncoder frameEncoder = new FrameEncoder();

    public void encode(
            Request request,
            OutputStream outputStream
    ) throws IOException {

        byte[] payload = request.payload();

        int requestSize =
                Integer.BYTES +
                        Short.BYTES +
                        Integer.BYTES +
                        Integer.BYTES +
                        payload.length;

        java.io.ByteArrayOutputStream buffer =
                new java.io.ByteArrayOutputStream();

        DataOutputStream data =
                new DataOutputStream(buffer);

        data.writeInt(request.type());
        data.writeShort(request.version());
        data.writeInt(request.correlationId());
        data.writeInt(payload.length);
        data.write(payload);

        Frame frame = new Frame(buffer.toByteArray());

        frameEncoder.encode(frame, outputStream);
    }
}
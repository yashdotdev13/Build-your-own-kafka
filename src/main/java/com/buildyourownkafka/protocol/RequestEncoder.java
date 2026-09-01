package com.buildyourownkafka.protocol;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class RequestEncoder {

    private final FrameEncoder frameEncoder;

    public RequestEncoder(DataOutputStream output) {
        this.frameEncoder =
                new FrameEncoder(output);
    }

    public void encode(Request request)
            throws IOException {

        ByteArrayOutputStream buffer =
                new ByteArrayOutputStream();

        DataOutputStream data =
                new DataOutputStream(buffer);

        data.writeInt(request.type());

        data.writeShort(request.version());

        data.writeInt(request.correlationId());

        data.writeInt(request.payload().length);

        data.write(request.payload());

        Frame frame =
                new Frame(buffer.toByteArray());

        frameEncoder.encode(frame);
    }
}
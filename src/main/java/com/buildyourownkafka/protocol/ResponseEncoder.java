package com.buildyourownkafka.protocol;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ResponseEncoder {

    private final FrameEncoder frameEncoder;

    public ResponseEncoder(DataOutputStream output) {
        this.frameEncoder = new FrameEncoder(output);
    }

    public void encode(Response response) throws IOException {

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        DataOutputStream data = new DataOutputStream(buffer);

        data.writeInt(response.correlationId());
        data.writeInt(response.status());
        data.writeInt(response.payload().length);
        data.write(response.payload());

        Frame frame = new Frame(buffer.toByteArray());

        frameEncoder.encode(frame);
    }
}
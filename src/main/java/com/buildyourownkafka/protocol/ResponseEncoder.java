package com.buildyourownkafka.protocol;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class ResponseEncoder {

    private final FrameEncoder frameEncoder = new FrameEncoder();

    public void encode(Response response, OutputStream outputStream
    ) throws IOException {

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(buffer);


        data.writeInt(response.correlationId());
        data.writeInt(response.status());
        data.writeInt(response.payload().length);
        data.write(response.payload());

        Frame frame = new Frame(buffer.toByteArray());


        frameEncoder.encode(frame,outputStream);
    }
}
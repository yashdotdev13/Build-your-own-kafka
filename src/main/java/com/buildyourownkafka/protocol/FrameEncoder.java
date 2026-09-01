package com.buildyourownkafka.protocol;

import java.io.DataOutputStream;
import java.io.IOException;

public class FrameEncoder {

    private final DataOutputStream output;

    public FrameEncoder(DataOutputStream output) {
        this.output = output;
    }

    public void encode(Frame frame) throws IOException {

        output.writeInt(frame.length());
        output.write(frame.payload());

        output.flush();
    }
}
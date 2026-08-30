package com.buildyourownkafka.protocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class FrameEncoder {

    public void encode(Frame frame, OutputStream outputStream)
            throws IOException {

        DataOutputStream dataOutputStream =
                new DataOutputStream(outputStream);

        dataOutputStream.writeInt(frame.length());

        dataOutputStream.write(frame.payload());

        dataOutputStream.flush();
    }
}
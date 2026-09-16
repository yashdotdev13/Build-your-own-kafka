package com.buildyourownkafka.broker;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public record LeaveGroupResponsePayload(
        String memberId
) {

    public LeaveGroupResponsePayload {

        if (memberId == null || memberId.isBlank()) {
            throw new IllegalArgumentException(
                    "Member ID cannot be blank"
            );
        }
    }

    public byte[] encode() throws IOException {

        byte[] memberIdBytes =
                memberId.getBytes(StandardCharsets.UTF_8);

        ByteArrayOutputStream output =
                new ByteArrayOutputStream();

        DataOutputStream dataOutput =
                new DataOutputStream(output);

        dataOutput.writeInt(memberIdBytes.length);
        dataOutput.write(memberIdBytes);

        dataOutput.flush();

        return output.toByteArray();
    }

    public static LeaveGroupResponsePayload decode(
            byte[] payload
    ) throws IOException {

        if (payload == null) {
            throw new IllegalArgumentException(
                    "Payload cannot be null"
            );
        }

        DataInputStream input =
                new DataInputStream(
                        new ByteArrayInputStream(payload)
                );

        int memberIdLength =
                input.readInt();

        if (memberIdLength <= 0) {
            throw new IllegalArgumentException(
                    "Member ID cannot be empty"
            );
        }

        byte[] memberIdBytes =
                new byte[memberIdLength];

        input.readFully(memberIdBytes);

        String memberId =
                new String(
                        memberIdBytes,
                        StandardCharsets.UTF_8
                );

        return new LeaveGroupResponsePayload(
                memberId
        );
    }
}
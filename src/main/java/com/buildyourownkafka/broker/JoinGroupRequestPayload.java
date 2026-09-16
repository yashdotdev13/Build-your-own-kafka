package com.buildyourownkafka.broker;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public record JoinGroupRequestPayload(String groupId, String memberId, int partitionCount) {

    public byte[] encode() throws IOException {

        validate();
        byte[] groupIdBytes = groupId.getBytes(StandardCharsets.UTF_8);
        byte[] memberIdBytes = memberId.getBytes(StandardCharsets.UTF_8);
        int size = Integer.BYTES + groupIdBytes.length + Integer.BYTES + memberIdBytes.length + Integer.BYTES;
        java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream(size);
        DataOutputStream dataOutput = new DataOutputStream(output);
        dataOutput.writeInt(groupIdBytes.length);
        dataOutput.write(groupIdBytes);
        dataOutput.writeInt(memberIdBytes.length);
        dataOutput.write(memberIdBytes);
        dataOutput.writeInt(partitionCount);
        dataOutput.flush();
        return output.toByteArray();
    }

    public static JoinGroupRequestPayload decode(byte[] payload) throws IOException {

        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }

        DataInputStream input = new DataInputStream(new java.io.ByteArrayInputStream(payload));
        int groupIdLength = input.readInt();
        if (groupIdLength <= 0) {
            throw new IllegalArgumentException("Group ID cannot be empty");
        }

        byte[] groupIdBytes = new byte[groupIdLength];

        input.readFully(groupIdBytes);
        String groupId = new String(groupIdBytes, StandardCharsets.UTF_8);
        int memberIdLength = input.readInt();
        if (memberIdLength <= 0) {
            throw new IllegalArgumentException("Member ID cannot be empty");
        }

        byte[] memberIdBytes = new byte[memberIdLength];
        input.readFully(memberIdBytes);
        String memberId = new String(memberIdBytes, StandardCharsets.UTF_8);
        int partitionCount = input.readInt();
        return new JoinGroupRequestPayload(groupId, memberId, partitionCount);
    }
    private void validate() {

        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("Group ID cannot be blank");
        }
        if (memberId == null || memberId.isBlank()) {
            throw new IllegalArgumentException("Member ID cannot be blank");
        }
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("Partition count must be greater than zero");
        }
    }
}
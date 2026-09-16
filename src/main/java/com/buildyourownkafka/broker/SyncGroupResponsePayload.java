package com.buildyourownkafka.broker;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public record SyncGroupResponsePayload(String memberId, int generation, List<Integer> partitions) {

    public SyncGroupResponsePayload {

        if (memberId == null || memberId.isBlank()) {
            throw new IllegalArgumentException("Member ID cannot be blank");
        }
        if (generation < 0) {
            throw new IllegalArgumentException("Generation cannot be negative");
        }
        if (partitions == null) {
            throw new IllegalArgumentException("Partitions cannot be null");
        }
        for (Integer partition : partitions) {
            if (partition == null || partition < 0) {
                throw new IllegalArgumentException("Partition cannot be negative");
            }
        }
        partitions = List.copyOf(partitions);
    }

    public byte[] encode() throws IOException {

        byte[] memberIdBytes = memberId.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataOutputStream dataOutput = new DataOutputStream(output);

        // Member ID
        dataOutput.writeInt(memberIdBytes.length);
        dataOutput.write(memberIdBytes);

        // Generation
        dataOutput.writeInt(generation);

        // Number of partitions
        dataOutput.writeInt(partitions.size());

        // Partition IDs
        for (Integer partition : partitions) {
            dataOutput.writeInt(partition);
        }
        dataOutput.flush();
        return output.toByteArray();
    }

    public static SyncGroupResponsePayload decode(byte[] payload) throws IOException {
        if (payload == null) {
            throw new IllegalArgumentException("Payload cannot be null");
        }
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload));

        // Member ID
        int memberIdLength = input.readInt();
        if (memberIdLength <= 0) {
            throw new IllegalArgumentException("Member ID cannot be empty");
        }

        byte[] memberIdBytes = new byte[memberIdLength];
        input.readFully(memberIdBytes);
        String memberId = new String(memberIdBytes, StandardCharsets.UTF_8);
        // Generation
        int generation = input.readInt();

        // Partition count
        int partitionCount = input.readInt();
        if (partitionCount < 0) {
            throw new IllegalArgumentException("Partition count cannot be negative");
        }
        List<Integer> partitions = new ArrayList<>(partitionCount);

        // Partition IDs
        for (int i = 0; i < partitionCount; i++) {
            int partition = input.readInt();
            if (partition < 0) {
                throw new IllegalArgumentException("Partition cannot be negative");
            }
            partitions.add(partition);
        }
        return new SyncGroupResponsePayload(memberId, generation, partitions);
    }
}
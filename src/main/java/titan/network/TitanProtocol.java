/*
 * Copyright 2026 Ram Narayanan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */

package titan.network;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * {@code TitanProtocol} defines the communication protocol for the Titan network.
 * It specifies the structure of messages, including header information (version, opcode, flags, length)
 * and the payload, for various operations within the Titan ecosystem.
 * This class provides static utility methods for sending and receiving protocol-compliant messages.
 */
    public class TitanProtocol {

    // Constants for the Protocol Header
    /**
 * The current version of the Titan Protocol. This byte is included in the header
 * to ensure compatibility between communicating entities.
 */
    private static final byte CURRENT_VERSION = 1;

    // OpCodes
    /**
 * OpCode for a heartbeat message. Used to keep connections alive and signal liveness.
 */
    public static final byte OP_HEARTBEAT         = 0x01;
    /**
 * OpCode for registering a service or worker with the orchestrator.
 */
    public static final byte OP_REGISTER          = 0x02;
    /**
 * OpCode for submitting a single job to the Titan orchestrator.
 */
    public static final byte OP_SUBMIT_JOB        = 0x03;
    /**
 * OpCode for submitting a Directed Acyclic Graph (DAG) of jobs to the Titan orchestrator.
 */
    public static final byte OP_SUBMIT_DAG        = 0x04;
    /**
 * OpCode for deploying an application or service artifact.
 */
    public static final byte OP_DEPLOY            = 0x05;
    /**
 * OpCode for initiating the execution of a deployed application or job.
 */
    public static final byte OP_RUN               = 0x06;
    /**
 * OpCode for stopping a running application or job.
 */
    public static final byte OP_STOP              = 0x07;
    /**
 * OpCode for requesting general statistics about the system or a specific entity.
 */
    public static final byte OP_STATS             = 0x08;
    /**
 * OpCode for requesting statistics in JSON format.
 */
    public static final byte OP_STATS_JSON        = 0x09;
    /**
 * OpCode for unregistering a service or worker from the orchestrator.
 */
    public static final byte OP_UNREGISTER_SERVICE = 0x0A;
    /**
 * OpCode for clearing or resetting collected statistics.
 */
    public static final byte OP_CLEAN_STATS       = 0x0B;
    /**
 * OpCode for staging assets or data required for a job or deployment.
 */
    public static final byte OP_STAGE             = 0x0C;
    /**
 * OpCode for starting a specific service.
 */
    public static final byte OP_START_SERVICE     = 0x0D;
    /**
 * OpCode for forcefully terminating a worker process.
 */
    public static final byte OP_KILL_WORKER       = 0x11;
    /**
 * OpCode indicating that a job has completed its execution.
 */
    public static final byte OP_JOB_COMPLETE      = 0x12;
    /**
 * OpCode for initiating a stream of logs from a running process or service.
 */
    public static final byte OP_LOG_STREAM        = 0x15;
    /**
 * OpCode for requesting historical logs for a specific job or service.
 */
    public static final byte OP_GET_LOGS          = 0x16;
    /**
 * OpCode for uploading an asset (e.g., a binary, configuration file) to the system.
 */
    public static final byte OP_UPLOAD_ASSET      = 0x53;
    /**
 * OpCode for fetching an asset from the system.
 */
    public static final byte OP_FETCH_ASSET       = 0x54;
    /**
 * OpCode for sending a batch of log entries.
 */
    public static final byte OP_LOG_BATCH        = 0x17;
    /**
 * OpCode for running a job from an archived state or with archived data.
 */
    public static final byte OP_RUN_ARCHIVE        =0x18;
    /**
 * OpCode for starting a service from an archived state or with archived configurations.
 */
    public static final byte OP_START_SERVICE_ARCHIVE = 0x19;
    // Job Type Headers (Strings used inside payload)
    /**
 * String identifier for the 'run archive' job type, typically used within payload data.
 */
    public static final String RUN_ARCHIVE = "RUN_ARCHIVE";
    /**
 * String identifier for the 'start archive service' job type, typically used within payload data.
 */
    public static final String START_ARCHIVE_SERVICE = "START_ARCHIVE_SERVICE";
    // --- RESPONSE OPCODES ---
    /**
 * Response OpCode indicating a successful operation (acknowledgment).
 */
    public static final byte OP_ACK               = 0x50; // Success
    /**
 * Response OpCode indicating a failure or error during an operation.
 */
    public static final byte OP_ERROR             = 0x51; // Failure
    /**
 * Response OpCode for sending generic data or a string response.
 */
    public static final byte OP_DATA              = 0x52; // Generic Response String

    /**
 * OpCode for setting a key-value pair in a distributed key-value store.
 */
    public static final byte OP_KV_SET            = 0x60;;
    /**
 * OpCode for retrieving the value associated with a key from a distributed key-value store.
 */
    public static final byte OP_KV_GET            = 0x61;
    /**
 * OpCode for adding a member to a set associated with a key in a distributed key-value store.
 */
    public static final byte OP_KV_SADD           = 0x62;
    /**
 * OpCode for retrieving all members of a set associated with a key from a distributed key-value store.
 */
    public static final byte OP_KV_SMEMBERS       = 0x63;
    /**
 * OpCode for requesting the current status of a specific job.
 */
    public static final byte OP_GET_JOB_STATUS    = 0x55;
    /**
     * SEND: Wraps the payload in our 8-byte header.
     * [ Version(1) | OpCode(1) | Flags(1) | Spare(1) | Length(4) ] + [ Payload ]
     */
    /**
 * Sends a message over the network using the Titan Protocol.
 * The message is wrapped in an 8-byte header consisting of:
 * [ Version(1) | OpCode(1) | Flags(1) | Spare(1) | Length(4) ]
 * followed by the UTF-8 encoded payload.
 *
 * @param out The {@link DataOutputStream} to write the message to.
 * @param opCode The operation code for the message.
 * @param payload The string payload of the message.
 * @throws IOException If an I/O error occurs during writing to the stream.
 */
    public static void send(DataOutputStream out, byte opCode, String payload) throws IOException {
        byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);
        int len = payloadBytes.length;

        out.writeByte(CURRENT_VERSION);
        out.writeByte(opCode);
        out.writeByte(0x00);
        out.writeByte(0x00);
        out.writeInt(len);

        out.write(payloadBytes);
        out.flush();

        System.out.println("[TitanProto] Sent Op:" + opCode + " Len:" + len);
    }

    /**
 * Reads an incoming message from the network, parsing the Titan Protocol header and payload.
 * It expects an 8-byte header followed by the payload data.
 *
 * @param in The {@link DataInputStream} to read the message from.
 * @return A {@link TitanProtocol.TitanPacket} object containing the parsed opcode and payload.
 * @throws Exception If a version mismatch occurs, the packet is too large, or an I/O error occurs.
 */
    public static TitanPacket read(DataInputStream in) throws Exception {
        byte version = in.readByte();
        byte opCode = in.readByte();
        byte flags = in.readByte();
        byte spare = in.readByte();
        int len = in.readInt();

        if (version != CURRENT_VERSION) {
            throw new Exception("Version Mismatch! Server expects v" + CURRENT_VERSION);
        }

        if(len > 1024 * 1024 * 10){
            throw new Exception("Packet too large: " + len);
        }

        byte[] buffer = new byte[len];
        in.readFully(buffer);
        String payload = new String(buffer, StandardCharsets.UTF_8);
        return new TitanPacket(opCode, payload);
    }

    /**
 * Represents a parsed Titan Protocol packet, containing its operation code and payload.
 */
    public static class TitanPacket {
        /**
 * The operation code of the received packet.
 */
    public byte opCode;
        /**
 * The string payload of the received packet.
 */
    public String payload;

        /**
 * Constructs a new {@code TitanPacket}.
 *
 * @param op The operation code for the packet.
 * @param pl The payload string for the packet.
 */
    public TitanPacket(byte op, String pl) { this.opCode = op; this.payload = pl;}
    }

    /**
 * Main method for demonstrating the Titan Protocol's send and read functionality.
 * It simulates sending a heartbeat message with a payload and then reading it back
 * to verify the protocol implementation.
 *
 * @param args Command line arguments (not used).
 */
    public static void main(String[] args){
        try {
            ByteArrayOutputStream virtualNetwork = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(virtualNetwork);

            // 2. SEND: Write a message using the NEW Protocol (OpCode + Payload)
            String originalMessage = "Hello, Titan Orchestrator!";
            System.out.println("Sending: [OP_HEARTBEAT] " + originalMessage);

            // Change: Pass the OpCode explicitly
            TitanProtocol.send(out, TitanProtocol.OP_HEARTBEAT, originalMessage);

            // 3. RECEIVE: Read it back
            ByteArrayInputStream inputData = new ByteArrayInputStream(virtualNetwork.toByteArray());
            DataInputStream in = new DataInputStream(inputData);

            // Change: Receive a Packet object, not a String
            TitanProtocol.TitanPacket packet = TitanProtocol.read(in);

            System.out.println("Received OpCode: " + packet.opCode);
            System.out.println("Received Payload: " + packet.payload);

            // 4. Verify
            boolean payloadMatch = originalMessage.equals(packet.payload);
            boolean opCodeMatch = (packet.opCode == TitanProtocol.OP_HEARTBEAT);

            if (payloadMatch && opCodeMatch) {
                System.out.println("[OK] TEST PASSED: Protocols match!");
            } else {
                System.out.println("[FAIL] TEST FAILED: Data mismatch.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Hello");
    }
}

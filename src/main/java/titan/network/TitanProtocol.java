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

public class TitanProtocol {

    // Constants for the Protocol Header
    private static final byte CURRENT_VERSION = 1;

    // OpCodes
    public static final byte OP_HEARTBEAT         = 0x01;
    public static final byte OP_REGISTER          = 0x02;
    public static final byte OP_SUBMIT_JOB        = 0x03;
    public static final byte OP_SUBMIT_DAG        = 0x04;
    public static final byte OP_DEPLOY            = 0x05;
    public static final byte OP_RUN               = 0x06;
    public static final byte OP_STOP              = 0x07;
    public static final byte OP_STATS             = 0x08;
    public static final byte OP_STATS_JSON        = 0x09;
    public static final byte OP_UNREGISTER_SERVICE = 0x0A;
    public static final byte OP_CLEAN_STATS       = 0x0B;
    public static final byte OP_STAGE             = 0x0C;
    public static final byte OP_START_SERVICE     = 0x0D;
    public static final byte OP_KILL_WORKER       = 0x11;
    public static final byte OP_JOB_COMPLETE      = 0x12;
    public static final byte OP_LOG_STREAM        = 0x15;
    public static final byte OP_GET_LOGS          = 0x16;
    public static final byte OP_UPLOAD_ASSET      = 0x53;
    public static final byte OP_FETCH_ASSET       = 0x54;
    public static final byte OP_LOG_BATCH        = 0x17;
    public static final byte OP_RUN_ARCHIVE        =0x18;
    public static final byte OP_START_SERVICE_ARCHIVE = 0x19;
    // Job Type Headers (Strings used inside payload)
    public static final String RUN_ARCHIVE = "RUN_ARCHIVE";
    public static final String START_ARCHIVE_SERVICE = "START_ARCHIVE_SERVICE";
    // --- RESPONSE OPCODES ---
    public static final byte OP_ACK               = 0x50; // Success
    public static final byte OP_ERROR             = 0x51; // Failure
    public static final byte OP_DATA              = 0x52; // Generic Response String

    /**
     * SEND: Wraps the payload in our 8-byte header.
     * [ Version(1) | OpCode(1) | Flags(1) | Spare(1) | Length(4) ] + [ Payload ]
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

    public static class TitanPacket {
        public byte opCode;
        public String payload;

        public TitanPacket(byte op, String pl) { this.opCode = op; this.payload = pl;}
    }

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

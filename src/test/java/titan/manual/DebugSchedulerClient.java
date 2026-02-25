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

package titan.manual;

import titan.network.TitanProtocol;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class DebugSchedulerClient {
    public static void main(String[] args) {
        System.out.println("[DEBUG] Debugging Titan Server...");
        try (Socket socket = new Socket("127.0.0.1", 9090);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            // Test 1: JSON STATS
            System.out.println(">> Sending OP_STATS_JSON (Op: 9)...");
            TitanProtocol.send(out, TitanProtocol.OP_STATS_JSON, "");

            TitanProtocol.TitanPacket response = TitanProtocol.read(in);
            System.out.println("<< Received Op: " + response.opCode);
            System.out.println("<< Payload: " + response.payload);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
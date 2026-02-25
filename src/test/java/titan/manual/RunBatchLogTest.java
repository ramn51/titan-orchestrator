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
import titan.network.RpcWorkerServer;
import titan.scheduler.Scheduler;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

public class RunBatchLogTest {

    private static final int SCHEDULER_PORT = 9092; // Different port to avoid conflicts
    private static final int WORKER_PORT = 8083;

    public static void main(String[] args) throws Exception {
        System.out.println("🔥 STARTING LOG BATCH INTEGRATION TEST 🔥");

        // 1. SETUP: Create a "chatty" script that prints enough to trigger batching
        setupChattyFile();

        CountDownLatch clusterReady = new CountDownLatch(1);

        // --- THREAD 1: SCHEDULER ---
        new Thread(() -> {
            try {
                Scheduler s = new Scheduler(SCHEDULER_PORT);
                System.out.println("[TEST] Scheduler started on " + SCHEDULER_PORT);
                s.start();
            } catch (Exception e) { e.printStackTrace(); }
        }).start();

        Thread.sleep(1000);

        // --- THREAD 2: WORKER ---
        new Thread(() -> {
            try {
                RpcWorkerServer w = new RpcWorkerServer(WORKER_PORT, "localhost", SCHEDULER_PORT, "GENERAL", false);
                System.out.println("[TEST] Worker started on " + WORKER_PORT);
                w.start();
            } catch (Exception e) { e.printStackTrace(); }
        }).start();

        // Wait for registration
        Thread.sleep(2000);

        // --- THREAD 3: CLIENT VALIDATION ---

        // A. Submit Job with a SPECIFIC ID so we can query it immediately
        // Format: JOB_ID|FILENAME|ARGS (Using the new parser logic)
        String jobId = "JOB-BATCH-001";
        String payload = jobId + "|chatty.py";

        System.out.println("🧪 Submitting Job: " + payload);
        String response = sendCommand(TitanProtocol.OP_RUN, payload);
        System.out.println("<< Submit Response: " + response);

        // B. POLL FOR LOGS (Simulating a UI tailing the logs)
        // We loop 5 times, waiting 1 second between checks
        boolean logsFound = false;

        for (int i = 1; i <= 5; i++) {
            Thread.sleep(1000);
            System.out.println("\n[TEST] Polling logs (Attempt " + i + "/5)...");

            // Send OP_GET_LOGS
            String logs = sendCommand(TitanProtocol.OP_GET_LOGS, jobId);

            if (logs.trim().isEmpty()) {
                System.out.println("   (No logs yet...)");
            } else {
                logsFound = true;
                // Count lines to see if batching is working
                long lineCount = logs.lines().count();
                System.out.println("[SUCCESS] RECEIVED LOGS! Line count: " + lineCount);

                // Print the last line to see progress
                String lastLine = logs.substring(logs.lastIndexOf("\n") + 1);
                System.out.println("   Last Line: " + lastLine);

                // If we have > 50 lines, we know the Batch Flush worked!
                if (lineCount >= 50) {
                    System.out.println("[PASS] SUCCESS: Received full batch (>50 lines).");
                    break;
                }
            }
        }

        if (!logsFound) {
            throw new RuntimeException("[FAIL] TEST FAILED: No logs were ever received from the Scheduler.");
        }

        System.out.println("\n[OK] Test Passed. Closing down.");
        System.exit(0);
    }

    private static void setupChattyFile() throws IOException {
        // Ensure directory exists
        File root = new File("titan_workspace"); // Matches your ScriptHandler root
        if (!root.exists()) root.mkdirs();

        // Python script: Prints 70 lines.
        // Logic: 70 lines > 50 Batch Size. This FORCES a flush mid-execution.
        String py =
                "import time\n" +
                        "print('--- STARTING CHATTY SCRIPT ---')\n" +
                        "for i in range(70):\n" +
                        "    print(f'Log Message Number {i} - Data Payload')\n" +
                        "    time.sleep(0.05)\n" + // fast enough to finish in 3.5s
                        "print('--- FINISHED ---')";

        Files.write(Paths.get(root.getAbsolutePath(), "chatty.py"), py.getBytes());
    }

    private static String sendCommand(byte opCode, String payload) {
        try (Socket socket = new Socket("localhost", SCHEDULER_PORT);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            TitanProtocol.send(out, opCode, payload);
            TitanProtocol.TitanPacket packet = TitanProtocol.read(in);
            return packet.payload;

        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }
}
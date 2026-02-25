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

import titan.network.RpcWorkerServer;
import titan.network.TitanProtocol;
import titan.scheduler.Scheduler;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class LoadBalancerTest {
    public static void main(String[] args) throws Exception {
        System.out.println("=== [START] STARTING LOAD BALANCER TEST ===");

        // 1. Start Scheduler
        Scheduler scheduler = new Scheduler(9090);
        scheduler.start();
        Thread.sleep(1000);

        // 2. Start TWO Workers
        System.out.println("[INFO] Starting Worker A (8080) and Worker B (8081)...");
        RpcWorkerServer workerA = new RpcWorkerServer(8080, "localhost", 9090, "PDF_CONVERT", false);
        RpcWorkerServer workerB = new RpcWorkerServer(8081, "localhost", 9090, "PDF_CONVERT", false);

        new Thread(() -> { try { workerA.start(); } catch (Exception e) {} }).start();
        new Thread(() -> { try { workerB.start(); } catch (Exception e) {} }).start();
        Thread.sleep(2000);

        // 3. Submit Job 1 (Worker A should pick this up as it's the first in the list)
        System.out.println("\n[Step 1] Sending Job 1...");
        submitJob("SUBMIT PDF_CONVERT|long_task_1.pdf|1|0");

        // Wait for Heartbeat to sync the 'Load=1' status back to Scheduler
        System.out.println("[WAITING] Waiting for Heartbeat to update load metrics (11s)...");
        Thread.sleep(11000);

        // 4. Submit Job 2
        // Since Worker A has Load 1 and Worker B has Load 0, B MUST be picked.
        System.out.println("\n[Step 2] Sending Job 2 (Should go to Worker B)...");
        submitJob("SUBMIT PDF_CONVERT|job_2.pdf|1|0");

        Thread.sleep(5000);
        System.out.println("\n=== 🛑 TEST FINISHED ===");
        System.exit(0);
    }

    private static void submitJob(String payload) {
        try (Socket client = new Socket("localhost", 9090);
             DataOutputStream out = new DataOutputStream(client.getOutputStream());
             DataInputStream in = new DataInputStream(client.getInputStream())) {

            TitanProtocol.send(out, TitanProtocol.OP_SUBMIT_JOB, payload);

            // FIX 2: Read Packet object
            TitanProtocol.TitanPacket ack = TitanProtocol.read(in);
            System.out.println("   [Client] Received Ack: " + ack.payload);
        } catch (Exception e) { e.printStackTrace(); }
    }
}
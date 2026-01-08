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
import titan.scheduler.Scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class WorkerLoadTest {

    public static void main(String[] args) throws Exception {
        System.out.println("=== [INFO] STARTING LOAD TEST ===");

        // 1. Start Scheduler
        Scheduler scheduler = new Scheduler(9090);
        scheduler.start();
        Thread.sleep(1000); // Wait for Scheduler to be ready

        // 2. Launch 10 Workers in Parallel
        int WORKER_COUNT = 10;
        int BASE_PORT = 8081;

        // CountDownLatch lets us wait until all threads are ready before checking
        CountDownLatch bootLatch = new CountDownLatch(WORKER_COUNT);
        List<RpcWorkerServer> workers = new ArrayList<>();

        for (int i = 0; i < WORKER_COUNT; i++) {
            int port = BASE_PORT + i;
            String capability = (i % 2 == 0) ? "PDF_CONVERT" : "EMAIL_SEND"; // Mix skills

            new Thread(() -> {
                try {
                    // Start worker (This blocks, so we run in a thread)
                    RpcWorkerServer worker = new RpcWorkerServer(port, "localhost", 9090, capability, false);
                    workers.add(worker); // Keep ref to stop later

                    // We can't easily know EXACTLY when start() finishes registration inside this thread
                    // without modifying Worker code, so we'll just sleep a bit in the main thread.
                    worker.start();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    bootLatch.countDown();
                }
            }).start();
        }

        // 3. Wait for registrations to complete
        System.out.println("⏳ Waiting for " + WORKER_COUNT + " workers to register...");
        Thread.sleep(3000); // Give them 3 seconds to finish handshakes

        // 4. Verify Registry Size
        int registeredCount = scheduler.getWorkerRegistry().getWorkers().size();
        System.out.println("📊 Registry Count: " + registeredCount + " / " + WORKER_COUNT);

        if (registeredCount == WORKER_COUNT) {
            System.out.println("[OK] LOAD TEST PASSED: All workers registered concurrently.");
        } else {
            System.err.println("[FAIL] LOAD TEST FAILED: Missing workers!");
        }

        // 5. Cleanup
        System.out.println("🛑 Shutting down cluster...");
        for (RpcWorkerServer w : workers) w.stop();
        scheduler.stop();
        System.exit(0);
    }
}
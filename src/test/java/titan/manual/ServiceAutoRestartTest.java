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
import titan.tasks.ServiceHandler;

import java.io.*;
import java.nio.file.*;

/**
 * Manual test for ServiceHandler auto-restart behavior.
 *
 * Tests two cases:
 *   1. A service that crashes should be automatically restarted.
 *   2. A service that is intentionally stopped should NOT be restarted.
 *
 * Does not require a live Titan cluster. RpcWorkerServer is constructed
 * pointing at a non-existent port — the constructor only initializes fields.
 * LogBatcher and notifyMasterOfServiceStop will silently fail their TCP calls,
 * which does not affect the test outcome.
 *
 * Run from the project root so that titan_workspace/ exists.
 */
public class ServiceAutoRestartTest {

    // Dummy server — constructor does not connect, safe to point at a dead port
    private static final RpcWorkerServer DUMMY_SERVER =
            new RpcWorkerServer(9999, "localhost", 19999, "GENERAL", false);

    public static void main(String[] args) throws Exception {
        System.out.println("=== SERVICE AUTO-RESTART TEST ===\n");

        boolean t1 = testCrashTriggersRestart();
        boolean t2 = testIntentionalStopNoRestart();

        System.out.println("\n=== RESULTS ===");
        System.out.println("  [TEST 1] Crash triggers restart : " + (t1 ? "PASSED" : "FAILED"));
        System.out.println("  [TEST 2] Intentional stop, no restart: " + (t2 ? "PASSED" : "FAILED"));

        if (!t1 || !t2) {
            System.exit(1);
        }
        System.out.println("\n  All tests passed.");
    }

    // ── Test 1: crash should trigger restart ──────────────────────────────────

    static boolean testCrashTriggersRestart() throws Exception {
        System.out.println("[TEST 1] Crash should trigger restart...");

        // Each time the script runs it appends a line to this file
        File counterFile = File.createTempFile("restart_counter_", ".txt");
        counterFile.deleteOnExit();

        // Script: record start, then exit immediately (simulates crash)
        File script = File.createTempFile("crash_svc_", ".py");
        script.deleteOnExit();
        writePythonScript(script,
                "with open('" + escapePath(counterFile) + "', 'a') as f:\n" +
                "    f.write('started\\n')\n" +
                "# exits immediately — crash\n"
        );

        String serviceId = "test-crash-svc-" + System.currentTimeMillis();
        ServiceHandler handler = new ServiceHandler("START", DUMMY_SERVER);
        String result = handler.execute(script.getAbsolutePath() + "|" + serviceId + "|0");
        System.out.println("  Deploy: " + result);

        if (!result.contains("DEPLOYED_SUCCESS")) {
            System.out.println("  FAIL: expected DEPLOYED_SUCCESS");
            return false;
        }

        // Wait: initial run (fast) + 3s restart delay + second run
        System.out.println("  Waiting 7s for crash detection + restart...");
        Thread.sleep(7000);

        long count = countLines(counterFile);
        System.out.println("  Start count: " + count + " (need >= 2)");

        // Clean up
        new ServiceHandler("STOP", DUMMY_SERVER).execute(serviceId);

        if (count < 2) {
            System.out.println("  FAIL: service did not restart after crash");
            return false;
        }
        System.out.println("  PASS");
        return true;
    }

    // ── Test 2: intentional stop should NOT restart ───────────────────────────

    static boolean testIntentionalStopNoRestart() throws Exception {
        System.out.println("\n[TEST 2] Intentional stop should NOT restart...");

        File counterFile = File.createTempFile("stop_counter_", ".txt");
        counterFile.deleteOnExit();

        // Script: record start, then sleep — will be killed by stop command
        File script = File.createTempFile("stop_svc_", ".py");
        script.deleteOnExit();
        writePythonScript(script,
                "import time\n" +
                "with open('" + escapePath(counterFile) + "', 'a') as f:\n" +
                "    f.write('started\\n')\n" +
                "time.sleep(300)\n"
        );

        String serviceId = "test-stop-svc-" + System.currentTimeMillis();
        ServiceHandler startHandler = new ServiceHandler("START", DUMMY_SERVER);
        String result = startHandler.execute(script.getAbsolutePath() + "|" + serviceId + "|0");
        System.out.println("  Deploy: " + result);

        if (!result.contains("DEPLOYED_SUCCESS")) {
            System.out.println("  FAIL: expected DEPLOYED_SUCCESS");
            return false;
        }

        // Let it start and write its counter entry
        Thread.sleep(1500);

        // Intentional stop
        ServiceHandler stopHandler = new ServiceHandler("STOP", DUMMY_SERVER);
        String stopResult = stopHandler.execute(serviceId);
        System.out.println("  Stop: " + stopResult);

        if (!stopResult.contains("STOPPED")) {
            System.out.println("  FAIL: expected STOPPED");
            return false;
        }

        // Wait past the 3s restart window
        System.out.println("  Waiting 5s past the restart window...");
        Thread.sleep(5000);

        long count = countLines(counterFile);
        System.out.println("  Start count: " + count + " (need exactly 1)");

        if (count != 1) {
            System.out.println("  FAIL: service restarted after intentional stop (count=" + count + ")");
            return false;
        }
        System.out.println("  PASS");
        return true;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static void writePythonScript(File f, String content) throws IOException {
        try (FileWriter fw = new FileWriter(f)) {
            fw.write(content);
        }
    }

    /** Escape backslashes for embedding a Windows path inside a Python string literal. */
    private static String escapePath(File f) {
        return f.getAbsolutePath().replace("\\", "\\\\");
    }

    private static long countLines(File f) throws IOException {
        if (!f.exists() || f.length() == 0) return 0;
        return Files.lines(f.toPath()).count();
    }
}

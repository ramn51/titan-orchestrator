package tests;

import network.RpcWorkerServer;
import network.TitanProtocol;
import scheduler.Scheduler;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class FaultToleranceTest {

    public static void main(String[] args) throws Exception {
        System.out.println("=== 🧪 STARTING COMPREHENSIVE FAULT TOLERANCE TEST ===");

        // 1. Start Scheduler
        Scheduler scheduler = new Scheduler(9090);
        scheduler.start();
        Thread.sleep(1000);

        // ==========================================
        // SCENARIO 1: THE CRASH & RECOVERY (Resilience)
        // ==========================================
        System.out.println("\n--- 🎬 SCENARIO 1: Worker Crash & Recovery ---");

        // Start Worker 1 (The Victim)
        RpcWorkerServer worker1 = new RpcWorkerServer(8080, "localhost", 9090, "PDF_CONVERT");
        new Thread(() -> {
            try { worker1.start(); } catch (Exception e) { e.printStackTrace(); }
        }).start();
        Thread.sleep(1000);

        // Submit Job 1
        submitJob("PDF_CONVERT|important_doc.pdf");

        // KILL WORKER 1 immediately!
        System.out.println("🔪 KILLING WORKER 1 (8080) NOW!");
        worker1.stop();

        Thread.sleep(3000); // Wait for Scheduler to retry and fail

        // Start Worker 2 (The Savior)
        System.out.println("🚑 Starting Backup Worker 2 (8081)...");
        RpcWorkerServer worker2 = new RpcWorkerServer(8081, "localhost", 9090, "PDF_CONVERT");
        new Thread(() -> {
            try { worker2.start(); } catch (Exception e) { e.printStackTrace(); }
        }).start();

        Thread.sleep(4000); // Wait for success

        // ==========================================
        // SCENARIO 2: POISON PILL (Dead Letter Queue)
        // ==========================================
        System.out.println("\n--- 🎬 SCENARIO 2: Poison Pill (Max Retries -> DLQ) ---");

        // We need a "Bad Worker" that always says NO.
        // Instead of writing a new class, we simulate it with a raw socket listener.
        Thread badWorkerThread = new Thread(() -> {
            try (ServerSocket badServer = new ServerSocket(8082)) {
                // Register manually as a worker
                try (Socket regSocket = new Socket("localhost", 9090);
                     DataOutputStream out = new DataOutputStream(regSocket.getOutputStream());
                     DataInputStream in = new DataInputStream(regSocket.getInputStream())) {
                    TitanProtocol.send(out, TitanProtocol.OP_REGISTER, "8082||PDF_CONVERT");
                    TitanProtocol.read(in); // Wait for ACK Packet
                    System.out.println("😈 Bad Worker (8082) Registered.");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                // Accept connections and ALWAYS fail
                while (!Thread.currentThread().isInterrupted()) {
                    Socket conn = badServer.accept();
                    DataInputStream in = new DataInputStream(conn.getInputStream());
                    DataOutputStream out = new DataOutputStream(conn.getOutputStream());

                    TitanProtocol.TitanPacket req = TitanProtocol.read(in);

                    // FIX: Check OpCode
                    if (req.opCode == TitanProtocol.OP_HEARTBEAT) {
                        TitanProtocol.send(out, TitanProtocol.OP_ACK, "PONG");
                    } else {
                        System.out.println("😈 Bad Worker rejecting: " + req.payload);
                        // FIX: Send OP_ERROR to trigger retry logic immediately
                        TitanProtocol.send(out, TitanProtocol.OP_ERROR, "JOB_FAILED_POISON_PILL");
                    }
                    conn.close();
                }
            } catch (Exception e) {
                // Silent exit on close
            }
        });
        badWorkerThread.start();
        Thread.sleep(1000);

        // Submit Job 2 (The Poison Pill)
        submitJob("PDF_CONVERT|poison_pill.pdf");

        // Wait for 3 retries to happen
        System.out.println("⏳ Waiting for 3 retries + DLQ move (approx 5s)...");
        Thread.sleep(6000);

        System.out.println("\n=== 🛑 TEST FINISHED ===");
        worker1.stop();
        worker2.stop();
        badWorkerThread.interrupt();
        scheduler.stop();
        System.exit(0);
    }

    private static void submitJob(String payload) {
        try (Socket client = new Socket("localhost", 9090);
             DataOutputStream out = new DataOutputStream(client.getOutputStream());
             DataInputStream in = new DataInputStream(client.getInputStream())) {
            // Use OP_SUBMIT_JOB, remove "SUBMIT " string prefix
            TitanProtocol.send(out, TitanProtocol.OP_SUBMIT_JOB, payload);

            // Read Packet
            TitanProtocol.TitanPacket ack = TitanProtocol.read(in);
            System.out.println("User Submitted: " + payload + " | Ack: " + ack.payload);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
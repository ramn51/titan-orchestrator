package tests;

import network.RpcWorkerServer;
import network.TitanProtocol;
import scheduler.Scheduler;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class CronAndPriorityTest {
    public static void main(String[] args) throws Exception {
        System.out.println("=== ⏳ STARTING CRON & PRIORITY TEST ===");

        // 1. Start Scheduler
        Scheduler scheduler = new Scheduler(9090);
        scheduler.start();
        Thread.sleep(1000);

        // 2. Start a Worker (Simulated Slow Worker to let queue build up)
        // We simulate a worker that takes 3 seconds per job
        RpcWorkerServer worker = new RpcWorkerServer(8080, "localhost", 9090, "PDF_CONVERT");
        new Thread(() -> {
            try { worker.start(); } catch (Exception e) { e.printStackTrace(); }
        }).start();

        // 3. Submit Jobs (Order is crucial to prove the logic)

        // Job A: BLOCKER (Low Priority, Runs Now)
        // This hogs the worker immediately, forcing B and C to wait in the queue.
        System.out.println("\n1️⃣ Submitting Blocker (Low Priority)...");
        submitJob("SUBMIT PDF_CONVERT|blocker.pdf|0|0");

        // Job B: FUTURE JOB (Normal Priority, Delay 8s)
        // Should go to Waiting Room, NOT Queue.
        System.out.println("2️⃣ Submitting Future Job (Delay 8s)...");
        submitJob("SUBMIT PDF_CONVERT|future_report.pdf|1|8000");

        // Job C: VIP JOB (High Priority, Delay 0)
        // Should jump ahead of any other pending normal jobs (if we had them).
        System.out.println("3️⃣ Submitting VIP Job (High Priority)...");
        submitJob("SUBMIT PDF_CONVERT|vip_urgent.pdf|2|0");

        // EXPECTED TIMELINE:
        // T+0s: Blocker runs. VIP waits in PriorityQueue. Future waits in WaitingRoom.
        // T+3s: Blocker finishes. VIP runs immediately (Priority logic).
        // T+6s: VIP finishes. Worker idle.
        // T+8s: Future Job wakes up, moves to Queue, runs.

        Thread.sleep(12000); // Wait enough time for everything to finish

        System.out.println("\n=== 🛑 TEST FINISHED ===");
        worker.stop();
        scheduler.stop();
        System.exit(0);
    }

    private static void submitJob(String rawPayload) {
        try (Socket client = new Socket("localhost", 9090);
             DataOutputStream out = new DataOutputStream(client.getOutputStream());
             DataInputStream in = new DataInputStream(client.getInputStream())) {

            String cleanPayload = rawPayload.replace("SUBMIT ", "");
            TitanProtocol.send(out, TitanProtocol.OP_SUBMIT_JOB, cleanPayload);
            TitanProtocol.TitanPacket ackPacket = TitanProtocol.read(in);
            System.out.println("   [Client] Sent: " + cleanPayload + " | Ack: " + ackPacket.payload);
        } catch (Exception e) { e.printStackTrace(); }
    }
}
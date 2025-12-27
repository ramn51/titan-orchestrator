package tests;

import network.RpcWorkerServer;
import network.TitanProtocol;
import scheduler.Scheduler;
import scheduler.WorkerRegistry;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class WorkerRegistrationTest {

    public static void main(String[] args) throws Exception {
        Scheduler scheduler = new Scheduler(9090);
        scheduler.start();

        // Give it a moment to boot
        Thread.sleep(500);

        // 2. Start a Worker (8080) and tell it to register with 9090
        // (Make sure your RpcWorkerServer main/constructor is updated as we discussed)
        RpcWorkerServer worker = new RpcWorkerServer(8080, "localhost", 9090, "PDF_CONVERT");
        new Thread(() -> {
            try { worker.start(); } catch (Exception e) { e.printStackTrace(); }
        }).start();

        Thread.sleep(1000); // Wait for handshake

        // 3. VERIFY REGISTRATION
        WorkerRegistry registry = scheduler.getWorkerRegistry();
        boolean isRegistered = registry.getWorkers().stream()
                .anyMatch(w -> w.port() == 8080 && w.capabilities().contains("PDF_CONVERT"));

        if (isRegistered) {
            System.out.println("[OK] TEST 1 PASSED: Worker Registered!");
        } else {
            System.err.println("[FAIL] TEST 1 FAILED: Worker not found in registry.");
        }

        // 4. VERIFY JOB SUBMISSION (Client Side)
        try (Socket client = new Socket("localhost", 9090);
             DataOutputStream out = new DataOutputStream(client.getOutputStream());
             DataInputStream in = new DataInputStream(client.getInputStream())) {


//            TitanProtocol.send(out, "SUBMIT PDF_CONVERT|doc1.pdf");
            TitanProtocol.send(out, TitanProtocol.OP_SUBMIT_JOB, "PDF_CONVERT|doc1.pdf");
            TitanProtocol.TitanPacket resp = TitanProtocol.read(in);

            if ("JOB_ACCEPTED".equals(resp.payload)) {
                System.out.println("[OK] TEST 2 PASSED: Job Accepted!");
            } else {
                System.err.println("[FAIL] TEST 2 FAILED: " + resp);
            }
        }

        System.exit(0);
    }
}
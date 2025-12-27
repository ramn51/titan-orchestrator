package tests;

import network.RpcWorkerServer;
import network.TitanProtocol;
import scheduler.Scheduler;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class EndToEndTest {

    public static void main(String[] args) throws Exception {
        System.out.println("=== 🎬 STARTING END-TO-END TEST ===");

        // 1. Start Scheduler (Port 9090)
        System.out.println("\n[1] Starting Scheduler...");
        Scheduler scheduler = new Scheduler(9090);
        scheduler.start();
        Thread.sleep(1000);

        // 2. Start Worker (Port 8080) with PDF Skill
        System.out.println("\n[2] Starting Worker...");
        RpcWorkerServer worker = new RpcWorkerServer(8080, "localhost", 9090, "PDF_CONVERT");
        new Thread(() -> {
            try { worker.start(); } catch (Exception e) { e.printStackTrace(); }
        }).start();

        // Wait for registration
        Thread.sleep(2000);

        // 3. User Submits a Job
        System.out.println("\n[3] User Submitting Job...");
        try (Socket client = new Socket("localhost", 9090);
             DataOutputStream out = new DataOutputStream(client.getOutputStream());
             DataInputStream in = new DataInputStream(client.getInputStream())) {

            // Send Job
            String jobPayload = "PDF_CONVERT|important_report.docx";
            TitanProtocol.send(out, TitanProtocol.OP_SUBMIT_JOB, jobPayload);

            // Read Acknowledgement (This just means "Added to Queue")
            TitanProtocol.TitanPacket ackPacket = TitanProtocol.read(in);
            System.out.println("User received ack: " + ackPacket.payload);
        }

        // 4. Wait and Watch
        System.out.println("\n[4] Waiting for Execution (Watch the logs below)...");

        // We sleep here to give the Dispatch Loop time to pick it up and the Worker time to run it
        // The console logs are the proof of success.
        Thread.sleep(5000);

        System.out.println("\n=== 🛑 TEST FINISHED ===");
        worker.stop();
        scheduler.stop();
        System.exit(0);
    }
}
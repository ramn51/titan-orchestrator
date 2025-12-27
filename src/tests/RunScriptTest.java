package tests;

import network.TitanProtocol;
import network.RpcWorkerServer;
import scheduler.Scheduler;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

public class RunScriptTest {

    private static final int SCHEDULER_PORT = 9091;
    private static final int WORKER_PORT = 8082;

    public static void main(String[] args) throws Exception {
        // 1. SETUP: Create the script to run
        setupRunFile();

        CountDownLatch clusterReady = new CountDownLatch(1);

        // --- THREAD 1: SCHEDULER ---
        new Thread(() -> {
            try {
                Scheduler s = new Scheduler(SCHEDULER_PORT);
                s.start();
            } catch (Exception e) { e.printStackTrace(); }
        }).start();

        // Wait a moment for Scheduler to open port
        Thread.sleep(1000);

        // --- THREAD 2: WORKER ---
        new Thread(() -> {
            try {
                // <--- REGISTRATION HAPPENS HERE (See explanation below)
                RpcWorkerServer w = new RpcWorkerServer(WORKER_PORT, "localhost", SCHEDULER_PORT, "GENERAL");
                w.start();
                clusterReady.countDown();
            } catch (Exception e) { e.printStackTrace(); }
        }).start();

        // Wait for worker to register
        Thread.sleep(2000);

        // --- THREAD 3: CLIENT (The Test) ---
        System.out.println("🧪 Submitting RUN Job...");

        // Send RUN command
        // Protocol: RUN|filename
        String response = sendSchedulerCommand(TitanProtocol.OP_RUN, "calc.py");

        System.out.println("<< Scheduler Response: " + response);

        if (!response.contains("QUEUED") && !response.contains("ACCEPTED")) {
            throw new RuntimeException("Test Failed: Scheduler rejected job");
        }

        System.out.println("[OK] Job Submitted successfully.");
        System.out.println("👀 Watch the Console logs above for 'RESULT: COMPLETED|0|Result: 5050'");

        // Keep alive to allow job to finish executing
        Thread.sleep(5000);
        System.exit(0);
    }

    private static void setupRunFile() throws IOException {
        File permDir = new File("perm_files");
        if (!permDir.exists()) permDir.mkdir();

        // Simple Python script: Sum of 0..100
        String py = "print('Result: ' + str(sum(range(101))))";
        Files.write(Paths.get("perm_files", "calc.py"), py.getBytes());
    }

    private static String sendSchedulerCommand(byte opCode, String payload) throws IOException {
        try (Socket socket = new Socket("localhost", SCHEDULER_PORT);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            TitanProtocol.send(out, opCode, payload);

            // Read Packet
            TitanProtocol.TitanPacket packet = TitanProtocol.read(in);
            return packet.payload;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
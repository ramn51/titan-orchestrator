package tests;

import network.TitanProtocol;
import network.RpcWorkerServer;
import scheduler.Scheduler;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

public class DeployCommandIntegrationTest {

    private static final int SCHEDULER_PORT = 9090; // The brain
    private static final int WORKER_PORT = 8081;    // The muscle
    private static final int UDP_PORT = 9999;       // The proof

    public static void main(String[] args) throws InterruptedException {

        // 1. SETUP: Prepare the file system (perm_files)
        // The Scheduler looks here for files to deploy
        setupPermFiles();

        CountDownLatch systemReadyLatch = new CountDownLatch(1);

        // --- THREAD 1: THE SCHEDULER (Server) ---
        Thread schedulerThread = new Thread(() -> {
            try {
                System.out.println("🧠 [Scheduler Thread] Starting Scheduler Core...");
                Scheduler scheduler = new Scheduler(SCHEDULER_PORT);
                scheduler.start();

                // Allow some time for the server socket to bind
                Thread.sleep(1000);
                systemReadyLatch.countDown();

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        schedulerThread.start();

        // --- THREAD 2: THE WORKER (Agent) ---
        Thread workerThread = new Thread(() -> {
            try {
                // Wait for Scheduler to be up so we can register
                systemReadyLatch.await();

                System.out.println("👷 [Worker Thread] Starting Worker...");
                // Note: Worker connects to "localhost" at SCHEDULER_PORT
                RpcWorkerServer worker = new RpcWorkerServer(WORKER_PORT, "localhost", SCHEDULER_PORT, "GENERAL");
                worker.start();

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        workerThread.start();

        // Allow full cluster convergence (Worker registration)
        systemReadyLatch.await();
        Thread.sleep(2000);

        // --- THREAD 3: THE TEST CLIENT (Main Thread) ---
        System.out.println("🧪 [Test Thread] Connecting to Scheduler...");
        try {
            runTestScenario();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("❌ TEST FAILED");
            System.exit(1);
        } finally {
            System.out.println("🛑 Test Complete. Shutting down.");
            System.exit(0);
        }
    }

    private static void setupPermFiles() {
        try {
            // Ensure perm_files exists
            File permDir = new File("perm_files");
            if (!permDir.exists()) permDir.mkdir();

            // Create the Python script that listens on UDP
            String pythonScript =
                    "import socket, sys, time\n" +
                            "print('Starting UDP Service...', flush=True)\n" +
                            "sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)\n" +
                            "sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n" +
                            "sock.bind(('0.0.0.0', " + UDP_PORT + "))\n" +
                            "print('UDP_READY', flush=True)\n" +
                            "while True:\n" +
                            "    try:\n" +
                            "        data, addr = sock.recvfrom(1024)\n" +
                            "        print(f'--> Received Packet: {str(data)}', flush=True)\n" +
                            "        sock.sendto(b'PONG_DEPLOYED', addr)\n" +
                            "    except Exception as e:\n" +
                            "        print(f'Error: {e}', flush=True)\n";

            Path filePath = Paths.get("perm_files", "deploy_test_svc.py");
            Files.write(filePath, pythonScript.getBytes(StandardCharsets.UTF_8));
            System.out.println("✅ Created test file: " + filePath.toAbsolutePath());

        } catch (IOException e) {
            throw new RuntimeException("Failed to setup perm_files", e);
        }
    }

    private static void runTestScenario() throws Exception {
        String filename = "deploy_test_svc.py";

        // 1. SEND DEPLOY COMMAND TO SCHEDULER
        // Note: We talk to SCHEDULER_PORT now, not the Worker directly
        System.out.println(">> Sending DEPLOY command to Scheduler...");

        // Protocol: DEPLOY|filename
        String response = sendSchedulerCommand("DEPLOY|" + filename);

        System.out.println("<< Scheduler Response: " + response);

        // Expectation: The scheduler puts it in the queue.
        // It might return "DEPLOY_QUEUED" or "JOB_ACCEPTED" depending on your exact SchedulerServer code.
        if (!response.contains("QUEUED") && !response.contains("ACCEPTED")) {
            throw new RuntimeException("Scheduler rejected deployment: " + response);
        }

        System.out.println("✅ Deployment Queued. Waiting for Worker Execution...");

        // 2. VERIFY DEPLOYMENT (UDP)
        // We wait a bit longer here because:
        // Scheduler Queue -> Pick Job -> Stage File -> Ack -> Start Process -> Python Boot
        Thread.sleep(4000);

        String testPayload = "HELLO_FROM_TEST_CLIENT";
        verifyUdp(testPayload);
        System.out.println("✅ UDP Verification Passed - Code is running remotely!");
    }

    // --- REPLACEMENT FOR sendCommand using TitanProtocol ---
    private static String sendSchedulerCommand(String request) throws IOException {
        try (Socket socket = new Socket("localhost", SCHEDULER_PORT);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            // Use your Protocol class to write/read
            TitanProtocol.send(out, request);
            return TitanProtocol.read(in);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Reuse your exact UDP verification logic
    private static void verifyUdp(String message) throws Exception {
        System.out.println("📡 Probing UDP Port " + UDP_PORT + "...");
        try (DatagramSocket ds = new DatagramSocket()) {
            ds.setSoTimeout(5000); // 5s timeout to allow for slow python startup
            byte[] data = message.getBytes();
            InetAddress target = InetAddress.getByName("127.0.0.1");

            ds.send(new DatagramPacket(data, data.length, target, UDP_PORT));

            byte[] buf = new byte[1024];
            DatagramPacket dp = new DatagramPacket(buf, buf.length);
            ds.receive(dp);

            String reply = new String(dp.getData(), 0, dp.getLength());
            if (!reply.equals("PONG_DEPLOYED")) throw new RuntimeException("Wrong UDP Reply: " + reply);

            System.out.println("✅ Received UDP Reply: '" + reply + "'");
        }
    }
}
package tests;

import network.RpcWorkerServer;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;

public class WorkerScriptIntegrationTest {

    private static final int TEST_PORT = 8081; // Use a different port to avoid conflicts
    private static final int UDP_PORT = 9999;

    public static void main(String[] args) throws InterruptedException {

        // 1. Latch to ensure Server starts before Client connects
        CountDownLatch serverReadyLatch = new CountDownLatch(1);

        // --- THREAD 1: THE WORKER SERVER ---
        Thread workerThread = new Thread(() -> {
            try {
                // Initialize Worker (Mocking Scheduler Host as localhost for reg)
                RpcWorkerServer worker = new RpcWorkerServer(TEST_PORT, "localhost", 9090, "TEST_CAPABILITY");

                // We mock the start() method slightly to notify us when ready
                System.out.println("👷 [Worker Thread] Starting Server...");
                serverReadyLatch.countDown(); // Signal that we are initializing
                worker.start();

            } catch (Exception e) {
                // Ignore "Connection Refused" for scheduler registration in this test
                if (!e.getMessage().contains("Connection refused")) {
                    e.printStackTrace();
                }
            }
        });

        workerThread.start();

        // Wait for worker to be effectively "up"
        serverReadyLatch.await();
        Thread.sleep(1000); // Safety buffer for socket binding

        // --- THREAD 2: THE TEST CLIENT (Main Thread) ---
        System.out.println("🧪 [Test Thread] Connecting to Worker...");
        try {
            runTestScenario();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.exit(0); // Kill the worker thread
        }
    }

    private static void runTestScenario() throws Exception {
        // --- DEFINE PYTHON SCRIPT ---
        String pythonScript =
                "import socket, sys\n" +
                        "sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)\n" +

                        // 🔥 NEW LINE: Allow port reuse to prevent WinError 10048
                        "sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n" +

                        "sock.bind(('0.0.0.0', " + UDP_PORT + "))\n" +
                        "print('UDP_READY', flush=True)\n" +
                        "while True:\n" +
                        "    data, addr = sock.recvfrom(1024)\n" +
                        "    # Convert bytes to string safely for logging\n" +
                        "    print(f'--> Received Packet: {str(data)}', flush=True)\n" +
                        "    sock.sendto(b'PONG_THREAD', addr)\n";

        String base64Script = Base64.getEncoder().encodeToString(pythonScript.getBytes(StandardCharsets.UTF_8));
        String filename = "logger_test.py";
        String serviceId = "log_test_svc";

        // 1. STAGE
        String resp = sendCommand("EXECUTE STAGE_FILE|" + filename + "|" + base64Script);
        if (!resp.contains("FILE_SAVED")) throw new RuntimeException("Staging Failed: " + resp);
        System.out.println("✅ File Staged");

        // 2. START
        resp = sendCommand("EXECUTE START_SERVICE|" + filename + "|" + serviceId);
        if (!resp.contains("DEPLOYED_SUCCESS")) throw new RuntimeException("Start Failed: " + resp);
        System.out.println("✅ Service Started (PID: " + resp.split("PID: ")[1] + ")");

        // 3. VERIFY (UDP)
        Thread.sleep(1000);
        String testPayload = "TEST_PAYLOAD_XYZ";// Wait for Python
        verifyUdp(testPayload);
        System.out.println("✅ UDP Verification Passed");

        verifyLogs(serviceId, testPayload);

        // 4. STOP
        resp = sendCommand("EXECUTE STOP_SERVICE|" + serviceId);
        if (!resp.contains("STOPPED")) throw new RuntimeException("Stop Failed: " + resp);
        System.out.println("✅ Service Stopped");
    }

    private static String sendCommand(String text) throws IOException {
        try (Socket socket = new Socket("localhost", TEST_PORT);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            byte[] data = text.getBytes(StandardCharsets.UTF_8);
            out.writeInt(data.length);
            out.write(data);

            int len = in.readInt();
            byte[] resp = new byte[len];
            in.readFully(resp);
            return new String(resp, StandardCharsets.UTF_8);
        }
    }

    // Updated helper method to send specific text
    private static void verifyUdp(String message) throws Exception {
        try (DatagramSocket ds = new DatagramSocket()) {
            ds.setSoTimeout(2000);
            byte[] data = message.getBytes();

            // Force loopback to ensure Windows routing works
            InetAddress target = InetAddress.getByName("127.0.0.1");

            ds.send(new DatagramPacket(data, data.length, target, UDP_PORT));

            byte[] buf = new byte[1024];
            DatagramPacket dp = new DatagramPacket(buf, buf.length);
            ds.receive(dp);

            String reply = new String(dp.getData(), 0, dp.getLength());
            if (!reply.equals("PONG_THREAD")) throw new RuntimeException("Wrong UDP Reply: " + reply);

            System.out.println("📡 Sent UDP: '" + message + "' -> Received: '" + reply + "'");
        }
    }

    private static void verifyLogs(String serviceId, String expectedContent) throws IOException, InterruptedException {
        System.out.println("\n🔎 INSPECTING LOGS FOR: [" + expectedContent + "]");
        File logFile = new File("./titan_workspace/" + serviceId + ".log");

        int maxRetries = 10;
        for (int i = 0; i < maxRetries; i++) {
            if (!logFile.exists()) {
                Thread.sleep(500);
                continue;
            }

            // Read all lines
            java.util.List<String> lines = java.nio.file.Files.readAllLines(logFile.toPath());

            // Check for success
            for (String line : lines) {
                if (line.contains(expectedContent)) {
                    System.out.println("\t[FILE LOG] " + line);
                    System.out.println("✅ LOG VERIFIED: Found message after " + (i+1) + " attempt(s).");
                    return;
                }
            }

            // If we are on the LAST attempt, DUMP the file to see the error
            if (i == maxRetries - 1) {
                System.err.println("❌ LOG FAILURE DUMP START ----------------");
                for (String line : lines) {
                    System.err.println("\t" + line); // Print everything (including Python Errors)
                }
                System.err.println("❌ LOG FAILURE DUMP END ------------------");
            }

            System.out.println("\t(Attempt " + (i+1) + ") Message not found yet. Retrying...");
            Thread.sleep(500);
        }

        throw new RuntimeException("❌ LOG FAILED: Message '" + expectedContent + "' did not appear in logs.");
    }
}
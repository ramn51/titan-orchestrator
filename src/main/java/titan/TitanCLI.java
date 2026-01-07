package titan;


import titan.network.TitanProtocol;

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.file.Files;
import java.util.Base64;
import java.util.Scanner;

public class TitanCLI {
    private final String host;
    private final int port;
    private final Scanner scanner;

    public TitanCLI(String host, int port) {
        this.host = host;
        this.port = port;
        this.scanner = new Scanner(System.in);
    }

    private void printHelp() {
        System.out.println("Commands:");
        System.out.println("  upload <local_path>  - Upload a file to server storage (perm_files)");
        System.out.println("  deploy <filename> [port] - Start a service/worker using a file on server");
        System.out.println("  run <filename>       - Execute a one-off script existing on server");
        System.out.println("  stats                - View cluster status");
        System.out.println("  stop <service_id>    - Stop a running service");
        System.out.println("  shutdown <port>      - Kill a specific worker node");
        System.out.println("  dag <dag_string>     - Submit raw DAG");
        System.out.println("  exit");
    }

    public void start() {
        System.out.println("==========================================");
        System.out.println("    [INFO] TITAN DISTRIBUTED ORCHESTRATOR    ");
        System.out.println("==========================================");
        System.out.println("Connected to: " + host + ":" + port);
        System.out.println("Commands: stats, deploy, run <filename>, upload <local_path>, dag <dag_string>, exit");

        while (true) {
            System.out.print("\ntitan> ");
            String input = scanner.nextLine().trim();

            if (input.equalsIgnoreCase("exit")) break;
            if (input.equalsIgnoreCase("help")) {
                printHelp();
                continue;
            }
            if (input.isEmpty()) continue;

            handleCommand(input);
        }
    }

    private void handleCommand(String input) {
        byte opCode;
        String payload = "";

        // 1. Map String Commands to OpCodes
        if (input.equalsIgnoreCase("stats")) {
            opCode = TitanProtocol.OP_STATS;
        }
        else if (input.equalsIgnoreCase("json")) {
            opCode = TitanProtocol.OP_STATS_JSON;
        }
        else if (input.startsWith("upload ")) {
            String localPath = input.substring(7).trim();
            if (localPath.isEmpty()) {
                System.out.println("[FAIL] Usage: upload <local_path>");
                return;
            }
            handleUpload(localPath);
            return;
        }
        else if (input.startsWith("submit ")) {
            // Input: "submit PDF_CONVERT file.docx"
            // Desired Payload: "PDF_CONVERT|file.docx|1|0"
            String[] parts = input.substring(7).split(" ", 2);
            if (parts.length < 2) {
                System.out.println("[FAIL] Usage: submit <skill> <data>");
                return;
            }
            opCode = TitanProtocol.OP_SUBMIT_JOB;
            payload = parts[0] + "|" + parts[1] + "|1|0";
        }
        else if (input.startsWith("dag ")) {
            // Input: "dag S1|... ; S2|..."
            opCode = TitanProtocol.OP_SUBMIT_DAG;
            payload = input.substring(4).trim();
        }
        else if (input.startsWith("run ")) {
            // Input: "run calc.py GPU"
            // Split by whitespace
            String[] args = input.substring(4).trim().split("\\s+");

            if (args.length < 1 || args[0].isEmpty()) {
                System.out.println("[FAIL] Usage: run <filename> [requirement]");
                return;
            }

            String filename = args[0];
            // If the args are not given the server defaults it to GENERAL
            String requirement = (args.length > 1) ? args[1] : "";

            opCode = TitanProtocol.OP_RUN;

            // Payload sent to Server: "filename|GPU"
            payload = requirement.isEmpty() ? filename : filename + "|" + requirement;
        }
        else if (input.startsWith("deploy ")) {
            String[] args = input.substring(7).trim().split("\\s+");

            if (args.length < 1 || args[0].isEmpty()) {
                System.out.println("[FAIL] Usage: deploy <server_filename> [port] [requirement]");
                System.out.println("       deploy Worker.jar 8085 GPU");
                System.out.println("       deploy service.yaml");
                return;
            }

            String filename = args[0];
            String targetPort = (args.length > 1) ? args[1] : "";

            // Capture the Requirement (e.g., "GPU" or "HIGH_MEM")
            String requirement = (args.length > 2) ? args[2] : "";

            if (filename.endsWith(".yaml") || filename.endsWith(".yml")) {
                System.out.println("[INFO] Triggering Service Stack Deployment from '" + filename + "'...");
                // Port is usually empty for YAML, but we send what we have just in case. The entrypoint already exists in the yaml def
            } else {
                if (targetPort.isEmpty() && filename.equalsIgnoreCase("Worker.jar")) {
                    System.out.println("[WARN] No port specified for Worker.jar. Server will use default.");
                }
                System.out.println("[INFO] Deploying '" + filename + "'...");
                if (!requirement.isEmpty()) {
                    System.out.println("       Constraint: " + requirement);
                }
            }

            opCode = TitanProtocol.OP_DEPLOY;
            // Payload: "filename|port"
            payload = filename + "|" + targetPort+ "|" + requirement;
        }
        else if (input.startsWith("stop ")) {
            opCode = TitanProtocol.OP_STOP;
            payload = input.substring(5);
        }
        else if (input.startsWith("shutdown ")) {
            String[] parts = input.trim().split("\\s+");
            if (parts.length < 2) {
                System.out.println("[FAIL] Usage: shutdown <port>");
                return;
            }
            opCode = TitanProtocol.OP_KILL_WORKER;
            payload = parts[1];
        }

        else {
            System.out.println("[FAIL] Unknown Command. Type 'help'.");
            return;
        }

        String response = sendAndReceive(opCode, payload);
        System.out.println("[INFO] Server Response:\n" + response);
    }

    private void handleUpload(String localPath) {
        try {
            File file = new File(localPath);

            // 1. Create the specific payload using the helper
            String payload = createUploadPayload(file);

            // 2. Send it using the existing OP_UPLOAD_ASSET code
            String response = sendAndReceive(TitanProtocol.OP_UPLOAD_ASSET, payload);

            if (response.contains("SUCCESS")) {
                System.out.println("[SUCCESS] Uploaded: " + file.getName());
            } else {
                System.out.println("[FAIL] Server Error: " + response);
            }

        } catch (IOException e) {
            System.out.println("[ERROR] Failed to read file: " + e.getMessage());
        }
    }

    private String createUploadPayload(File file) throws IOException {
        if (!file.exists()) {
            throw new FileNotFoundException("Local file not found: " + file.getAbsolutePath());
        }

        byte[] fileBytes = Files.readAllBytes(file.toPath());
        String base64Content = Base64.getEncoder().encodeToString(fileBytes);

        // Format: FILENAME|BASE64_CONTENT
        // We use file.getName() so "C:/Users/Dev/Worker.jar" becomes just "Worker.jar"
        return file.getName() + "|" + base64Content;
    }

    private String sendAndReceive(byte opCode, String payload) {
        try (Socket socket = new Socket(host, port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            TitanProtocol.send(out, opCode, payload);

            TitanProtocol.TitanPacket response = TitanProtocol.read(in);
            return response.payload;

        } catch (IOException e) {
            return "[FAIL] Error: Could not reach Scheduler at " + host + ":" + port;
        } catch (Exception e) {
            return "[FAIL] Protocol Error: " + e.getMessage();
        }
    }

    public static void main(String[] args) {
        new TitanCLI("localhost", 9090).start();
    }
}
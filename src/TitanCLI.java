import network.TitanProtocol;
import network.TitanProtocol.TitanPacket; // Import Packet class
import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
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

    public void start() {
        System.out.println("==========================================");
        System.out.println("    [INFO] TITAN DISTRIBUTED ORCHESTRATOR    ");
        System.out.println("==========================================");
        System.out.println("Connected to: " + host + ":" + port);
        System.out.println("Commands: stats, json, submit <skill> <data>, dag <raw_dag>, exit");

        while (true) {
            System.out.print("\ntitan> ");
            String input = scanner.nextLine().trim();

            if (input.equalsIgnoreCase("exit")) break;
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
            payload = input.substring(4);
        }
        else if (input.startsWith("run ")) {
            // Input: "run calc.py"
            opCode = TitanProtocol.OP_RUN;
            payload = input.substring(4);
        }
        else if (input.startsWith("deploy ")) {
            // Input: "deploy worker.jar"
            opCode = TitanProtocol.OP_DEPLOY;
            payload = input.substring(7);
        }
        else if (input.startsWith("stop ")) {
            opCode = TitanProtocol.OP_STOP;
            payload = input.substring(5);
        }
        else if (input.startsWith("shutdown ")) {
            try {
                // Syntax: shutdown 8086
                String[] parts = input.split(" ");
                int targetPort = Integer.parseInt(parts[1]);

                System.out.println("Attempting to shutdown worker on port " + targetPort + "...");
                sendShutdownCommand("localhost", targetPort);
                return;

            } catch (NumberFormatException e) {
                System.err.println("Invalid port number.");
                return;
            }
        }

        else {
            System.out.println("[FAIL] Unknown Command. Try: stats, submit, dag, run, deploy");
            return;
        }

        String response = sendAndReceive(opCode, payload);
        System.out.println("[INFO] Server Response:\n" + response);
    }

    private static void sendShutdownCommand(String host, int port) {
        try (Socket socket = new Socket(host, port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            // 1. Construct the Payload
            // Must match the key in RpcWorkerServer's taskHandlerMap
            String payload = "SHUTDOWN_WORKER|NOW";

            // 2. Send the Packet
            // OpCode 5 = OP_RUN (Generic Execution)
            TitanProtocol.send(out, TitanProtocol.OP_RUN, payload);

            TitanProtocol.TitanPacket response = TitanProtocol.read(in);
            System.out.println("Worker responded: " + response.payload);
            System.out.println("Target worker on port " + port + " has been terminated.");

        } catch (ConnectException e) {
            System.err.println("[ERROR] Could not connect to port " + port + ". Is it already dead?");
        } catch (IOException e) {
            System.err.println("[ERROR] Error sending shutdown: " + e.getMessage());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String sendAndReceive(byte opCode, String payload) {
        try (Socket socket = new Socket(host, port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            TitanProtocol.send(out, opCode, payload);

            TitanPacket response = TitanProtocol.read(in);
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
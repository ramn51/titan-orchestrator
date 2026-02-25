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


package titan.network;

import titan.scheduler.Job;
import titan.scheduler.Scheduler;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import titan.network.TitanProtocol.TitanPacket;

/**
 * The {@code SchedulerServer} class acts as the central network endpoint for the Titan distributed scheduling system.
 * It listens for incoming client connections, processes various commands from workers and clients (e.g., registration,
 * job submission, asset management, statistics requests), and delegates core scheduling logic to an associated {@link titan.scheduler.Scheduler} instance.
 * <p>
 * This server operates in a multithreaded manner, using an {@link java.util.concurrent.ExecutorService} to handle each client connection concurrently,
 * ensuring responsiveness and scalability.
 */
    public class SchedulerServer {
    private final int port;
    private boolean isRunning = true;
    private final ExecutorService threadPool;
    Scheduler scheduler;
    private final ServerSocket serverSocket;

    private static final String PERM_FILES_DIR = "perm_files";

    /**
 * Constructs a new {@code SchedulerServer} instance.
 * Initializes the server to listen on the specified port and associates it with the provided {@link titan.scheduler.Scheduler}.
 * A cached thread pool is created to manage client connections.
 *
 * @param port The port number on which the server will listen for incoming client connections.
 * @param scheduler The {@link titan.scheduler.Scheduler} instance responsible for managing workers, jobs, and system state.
 * @throws IOException If an I/O error occurs when attempting to open the server socket on the specified port.
 */
    public SchedulerServer(int port, Scheduler scheduler) throws IOException {
        this.port = port;
        threadPool = Executors.newCachedThreadPool();
        this.scheduler = scheduler;
        this.serverSocket = new ServerSocket(this.port);
    }

    /**
 * Starts the {@code SchedulerServer}, making it listen for incoming client connections.
 * This method enters a continuous loop, accepting new client sockets and submitting them to the internal thread pool
 * for asynchronous handling by the {@link #clientHandler(Socket)} method.
 * The server will continue to run until the {@link #stop()} method is called or a critical error occurs.
 * System messages are printed to indicate the server's listening status.
 */
    public void start(){
        try(serverSocket){
            System.out.println("[OK] SchedulerServer Listening on port " + port);
            while(isRunning){
                try{
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Incoming connection from " + clientSocket.getInetAddress() + " Port" + clientSocket.getPort());
                    threadPool.submit(() -> clientHandler(clientSocket));
                } catch (IOException e){
                    e.printStackTrace();
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
 * Handles a worker registration request from a client.
 * This method parses the registration payload, extracts the worker's port, capabilities, and permanence status,
 * and then registers the worker with the central {@link titan.scheduler.Scheduler}.
 *
 * @param socket The {@link Socket} object representing the client connection, used to determine the worker's host address.
 * @param request The payload string containing registration details, typically in the format "workerPort||capability||isPerm".
 * @return A string indicating the outcome of the registration, e.g., "REGISTERED" on success or "ERROR_INVALID_REGISTRATION" on failure.
 */
    private String handleRegistration(Socket socket, String request){
            String[] parts = request.split("\\|\\|");
            if (parts.length < 1) return "ERROR_INVALID_REGISTRATION";
            int workerPort = Integer.parseInt(parts[0]);

            String capability = (parts.length > 1) ? parts[1] : "GENERAL";
            String host = socket.getInetAddress().getHostAddress();

            boolean isPerm = false;
            if (parts.length > 2) {
                isPerm = Boolean.parseBoolean(parts[2]);
            }

            System.out.println("Registering Worker: " + host + " with " + capability);
            scheduler.registerWorker(host, workerPort, capability, isPerm);
//            main.java.titan.scheduler.getWorkerRegistry().addWorker(host, workerPort, capability);
            return ("REGISTERED");
    }

    /**
 * Handles a single client connection from end-to-end.
 * This method reads an incoming {@link titan.network.TitanProtocol.TitanPacket}, dispatches it to the appropriate command processing method
 * based on its operation code, and then sends a response {@link titan.network.TitanProtocol.TitanPacket} back to the client.
 * It uses {@link java.io.DataInputStream} and {@link java.io.DataOutputStream} for binary protocol communication.
 * Errors during processing are caught, logged, and an error response is sent to the client.
 *
 * @param socket The {@link Socket} object representing the client connection to be handled.
 */
    public void clientHandler(Socket socket){
        try(socket;
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream())
        ){

            TitanPacket packet = TitanProtocol.read(in);
            String responsePayload;
            byte responseOpCode = TitanProtocol.OP_ACK;
            try {
                switch (packet.opCode) {
                    case TitanProtocol.OP_REGISTER:
                        responsePayload = handleRegistration(socket, packet.payload);
                        break;
                    default:
                        responsePayload = processCommand(packet);
                        break;
                }

//                if (responsePayload.startsWith("ERROR") || responsePayload.startsWith("UNKNOWN")) {
//                    responseOpCode = TitanProtocol.OP_ERROR;
//                }
            } catch (Throwable t) {
                t.printStackTrace();
                responsePayload = "SERVER_ERROR: " + t.getMessage();
                responseOpCode = TitanProtocol.OP_ERROR;
            }

            TitanProtocol.send(out, responseOpCode, responsePayload);

        } catch (IOException e) {
            System.err.println("Client Disconnected abruptly : " + e.getMessage());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
 * Processes a request to deploy a file (e.g., a JAR or script) to a worker.
 * It reads the specified file from the server's {@code perm_files} directory, encodes its content in Base64,
 * and constructs a {@code DEPLOY_PAYLOAD} job. This job is then submitted to the {@link titan.scheduler.Scheduler}
 * for execution on a suitable worker.
 *
 * @param fileName The name of the file to be deployed. This file must exist in the {@code perm_files} directory.
 * @param port The port number to associate with the deployed worker, primarily used when deploying a 'Worker.jar'. Can be {@code null} or empty.
 * @param requirement The capability requirement (e.g., "GPU", "GENERAL") for the worker that should execute this deployment job.
 * @return A string indicating the status of the deployment job, such as "DEPLOY_QUEUED" or an error message if the file is not found or an I/O issue occurs.
 */
    private String handleDeployRequest(String fileName, String port, String requirement) {
        try {
            File file = new File(PERM_FILES_DIR + File.separator + fileName);
            if (!file.exists()) {
                return "ERROR: File not found in " + PERM_FILES_DIR;
            }

            byte[] fileBytes = Files.readAllBytes(file.toPath());
            String base64Content = Base64.getEncoder().encodeToString(fileBytes);

            Job job = new Job("TEMP_PAYLOAD", 1, 0);
            String internalId = job.getId();

            if ((port == null || port.trim().isEmpty()) && fileName.equalsIgnoreCase("Worker.jar")) {
                port = "8085";
            }

            String safeReq = (requirement == null || requirement.isEmpty()) ? "GENERAL" : requirement;

            // If it's a Worker, we include the port in the ID for easy matching later
            String taggedId = (fileName.equalsIgnoreCase("Worker.jar"))
                    ? "WRK-" + port + "-" + internalId
                    : "TSK-" + internalId;

            // 3. Update the Job with the tagged ID and final payload
            job.setId(taggedId);

            // 2. Construct a Payload that contains EVERYTHING the worker needs
            // Format: DEPLOY_PAYLOAD|filename|base64data|port|requirement
            String payload = "DEPLOY_PAYLOAD|" + fileName + "|" + base64Content + "|" + port + "|" + safeReq;;
            job.setPayload(payload);
            scheduler.submitJob(job);

            System.out.println("[INFO] DEPLOY Job queued for file: " + fileName + " [Req: " + safeReq + "]");
            return "DEPLOY_QUEUED";

        } catch (IOException e) {
            e.printStackTrace();
            return "ERROR: Server File IO issue";
        }
    }


    /**
 * Handles a request to execute a script or an executable file on a worker.
 * This method searches for the specified file recursively within the {@code perm_files} directory,
 * encodes its content in Base64, and creates a {@code RUN_PAYLOAD} job. The job is then submitted
 * to the {@link titan.scheduler.Scheduler} for execution on a worker matching the specified requirements.
 *
 * @param fileName The name of the script or executable file to be run.
 * @param requirement The capability requirement (e.g., "PYTHON", "GENERAL") for the worker that should execute this run job.
 * @return A string indicating the status of the job, such as "JOB_QUEUED" along with the job ID, or an error message if the file is not found or an I/O issue occurs.
 */
    private String handleRunScript(String fileName, String requirement){
        try {
            String safeReq = (requirement == null || requirement.isEmpty()) ? "GENERAL" : requirement;
            System.out.println("[INFO] Looking for '" + fileName + "' in workspace...");
            File file = findFileRecursive(fileName);

            if (file == null || !file.exists()) {
                System.err.println("[ERROR] File not found: " + fileName);
                return "ERROR: File not found recursively in " + PERM_FILES_DIR;
            }

            System.out.println("[INFO] Found: " + file.getAbsolutePath());

            Job job = new Job("TEMP", 1, 0);
            String fullJobId = "TSK-" + job.getId();
            job.setId(fullJobId);

            byte[] fileBytes = Files.readAllBytes(file.toPath());
            String base64Content = Base64.getEncoder().encodeToString(fileBytes);

            if (base64Content == null) return "ERROR: File not found";

            String payload = "RUN_PAYLOAD|" + file.getName() + "|" + base64Content + "|" + safeReq;
            // Payload: RUN_PAYLOAD|filename|base64
//            Job job = new Job(payload, 1, 0);
            job.setPayload(payload);
            scheduler.submitJob(job);
            return "JOB_QUEUED" + fullJobId + " on " + safeReq;
        } catch (IOException e) {
            e.printStackTrace();
            return "ERROR: Server File IO issue";
        }
    }

    /**
 * Parses a string containing multiple job definitions for a Directed Acyclic Graph (DAG) and submits each job to the {@link titan.scheduler.Scheduler}.
 * Each job definition within the request string is expected to be separated by a semicolon ({@code ;}).
 * Jobs are parsed using {@link titan.scheduler.Job#fromDagString(String)}.
 *
 * @param request A string containing one or more job definitions, typically in a format like "job1_def;job2_def;..."
 *                where each definition can be parsed into a {@link titan.scheduler.Job} object.
 */
    private void parseAndSubmitDAG(String request){
        String [] jobs = request.split(";");
        for(String jobDef: jobs){
            if(jobDef.trim().isEmpty()) continue;
            try {
                Job job = Job.fromDagString(jobDef.trim());
                System.out.println("[INFO] [PARSER] Created Job: " + job.getId());
                scheduler.submitJob(job);
            } catch (Exception e) {
                System.err.println("[FAIL] Failed to parse DAG job: " + jobDef + " Error: " + e.getMessage());
            }
        }
    }


    /**
 * Dispatches incoming {@link titan.network.TitanProtocol.TitanPacket}s to the appropriate handler method based on their {@code opCode}.
 * This method acts as the central command router for all operations received from clients, excluding initial worker registration.
 * It supports a wide range of operations including deployment, script execution, system statistics, service management,
 * job submission (single and DAG), worker control, logging, asset transfer, and key-value store interactions.
 *
 * @param packet The {@link titan.network.TitanProtocol.TitanPacket} received from the client, containing the operation code and its payload.
 * @return A response string to be sent back to the client, indicating the result of the command execution or an error message.
 */
    private String processCommand(TitanPacket packet){
        String payload = packet.payload;
        String[] parts;

        switch (packet.opCode){
            case TitanProtocol.OP_DEPLOY:
                parts = payload.split("\\|");
                if (parts.length < 1) return "ERROR: Missing filename";
                String deployFile = parts[0];
                String deployPort = (parts.length > 1) ? parts[1] : "";
                String deployReq = (parts.length > 2) ? parts[2] : "GENERAL";

                return handleDeployRequest(deployFile, deployPort, deployReq);

            case TitanProtocol.OP_RUN:
                if (payload.isEmpty()) return "ERROR: Missing filename";
                // To handle skill based running based on worker capabaility (handled in Scheduler)
                String[] runParts = payload.split("\\|");
                String runFile = runParts[0];
                String runReq = (runParts.length > 1) ? runParts[1] : "GENERAL";
                return handleRunScript(runFile, runReq);

            case TitanProtocol.OP_STATS_JSON:
                System.out.println("[INFO] Generating JSON Stats...");
                return scheduler.getSystemStatsJSON();

            case TitanProtocol.OP_CLEAN_STATS:
                scheduler.getLiveServiceMap().clear();
                return "Stats Map Cleared. Run STATS again to see fresh state.";

            case TitanProtocol.OP_UNREGISTER_SERVICE:
                String serviceId = payload;
                scheduler.getLiveServiceMap().remove(serviceId);
                System.out.println("[INFO] Cleaned up service: " + serviceId);
                return "ACK_UNREGISTERED";

            case TitanProtocol.OP_STOP:
                if (payload.isEmpty()) return "ERROR: Missing Service ID";
                return scheduler.stopRemoteService(payload);

            case TitanProtocol.OP_STATS:
                return scheduler.getSystemStats();

            case TitanProtocol.OP_SUBMIT_DAG:
                parseAndSubmitDAG(payload);
                return "DAG_ACCEPTED";

            case TitanProtocol.OP_SUBMIT_JOB:
                scheduler.submitJob(payload);
                return "JOB_ACCEPTED";

            case TitanProtocol.OP_KILL_WORKER:
                try {
                    // OLD: int targetPort = Integer.parseInt(payload);
                    // NEW FORMAT: "HOST|PORT" (e.g., "192.168.1.5|8081" or "localhost|8081")
                    String[] killParts = payload.split("\\|");
                    if (killParts.length < 2) {
                        return "ERROR: Payload must be HOST|PORT";
                    }

                    String targetHost = killParts[0];
                    int targetPort = Integer.parseInt(killParts[1]);

                    return scheduler.shutdownWorkerNode(targetHost, targetPort);
                } catch (NumberFormatException e) {
                    return "ERROR: Invalid Port Format";
                }

            case TitanProtocol.OP_JOB_COMPLETE:
                scheduler.handleJobCallback(payload);
                return "ACK_CALLBACK";

            case TitanProtocol.OP_LOG_BATCH:
                // Payload format: "jobId|line1\nline2\nline3..."
                // Split Job ID from the massive text block
                String[] batchParts = payload.split("\\|", 2);

                if (batchParts.length == 2) {
                    String batchJobId = batchParts[0];
                    String fullLogBlock = batchParts[1];

                    // Split the block back into individual lines
                    // The Worker joined them with "\n", so we split by "\n"
                    String[] batchLines = fullLogBlock.split("\n");
                    for (String line : batchLines) {
                        scheduler.logStream(batchJobId, line);
                    }
                }
                return "ACK_BATCH";

            case TitanProtocol.OP_LOG_STREAM:
                // Payload format: "jobId|logLine"
                // Split into 2 parts max so the log message can contain pipes safely
                String[] logParts = payload.split("\\|", 2);
                if (logParts.length == 2) {
                    scheduler.logStream(logParts[0], logParts[1]);
                }
                return "ACK_LOG"; // send back an ack

            case TitanProtocol.OP_GET_LOGS:
                // Payload is the jobId in this case
                String jobId = payload;
                List<String> logs = scheduler.getLogs(jobId);
                if (logs.isEmpty()) {
//                    File logFile = new File("titan_workspace/shared/" + jobId + ".log");
                    File logFile = new File("titan_server_logs" + File.separator + jobId + ".log");
                    if (logFile.exists()) {
                        try {
                            return new String(Files.readAllBytes(logFile.toPath()));
                        } catch (IOException e) {
                            return "ERROR: Log file exists but unreadable";
                        }
                    }
                }
                return String.join("\n", logs);


            case TitanProtocol.OP_UPLOAD_ASSET:
                // Payload format: "FILENAME | BASE64_CONTENT"
                String uploadParts[] = payload.split("\\|", 2);
                if(uploadParts.length < 2) return "ERROR: Invalid Upload Payload";

                String assetName = uploadParts[0];
                String assetData = uploadParts[1];

                try{
                    File permDir = new File("perm_files");
                    if(!permDir.exists()) permDir.mkdirs();

                    byte[] decodedBytes = Base64.getDecoder().decode(assetData);
                    File destFile = new File(permDir, assetName);
                    Files.write(destFile.toPath(), decodedBytes);
                    System.out.println("[UPLOAD] Saved asset: " + assetName + " (" + decodedBytes.length + " bytes)");
                    return "UPLOAD_SUCCESS";
                }catch (Exception e) {
                    e.printStackTrace();
                    return "UPLOAD_FAILED: " + e.getMessage();
                }

            case TitanProtocol.OP_FETCH_ASSET:
                // Payload: "filename" (e.g., "my_project.zip")
                String requestedFile = payload.trim();
                File assetFile = new File("perm_files/" + requestedFile);

                if (!assetFile.exists()) {
                    return "ERROR_NOT_FOUND";
                }

                try {
                    byte[] fileBytes = Files.readAllBytes(assetFile.toPath());
                    return Base64.getEncoder().encodeToString(fileBytes);
                } catch (IOException e) {
                    return "ERROR_READING_FILE: " + e.getMessage();
                }

            case TitanProtocol.OP_KV_SET:
                parts = payload.split("\\|", 2);
                if(parts.length < 2) return "ERROR: Invalid Format";
                // Prefix 'user:' for safety
                scheduler.redisKVSet("user:" + parts[0], parts[1]);
                return "OK";

            case TitanProtocol.OP_KV_GET:
                String val = scheduler.redisKVGet("user:" + payload);
                return val == null ? "NULL" : val;

            // These are called and used by sdk
            case TitanProtocol.OP_KV_SADD:
                // Payload: key|member
                parts = payload.split("\\|", 2);
                if(parts.length < 2) return "ERROR: Invalid Format";
                scheduler.redisSetAdd("user:" + parts[0], parts[1]);
                return "0"; // Returns "1" (added) or "0" (duplicate)

            case TitanProtocol.OP_KV_SMEMBERS:
                // Payload: key
                java.util.Set<String> members = scheduler.safeRedisSMembers("user:" + payload);
                if (members == null || members.isEmpty()) {
                    return "";
                }
                
                // Flatten Set to CSV (e.g., "item1,item2,item3")
                return String.join(",", members);

            case TitanProtocol.OP_GET_JOB_STATUS:
                // Payload will be the full job ID (e.g., "DAG-test_job_123")
                String status = scheduler.redisKVGet("job:" + payload + ":status");
                return status == null ? "NULL" : status;


            default:
                return "UNKNOWN_OPCODE: " + packet.opCode;

        }
    }

    /**
 * Searches for a specified file within the {@code PERM_FILES_DIR} and its subdirectories.
 * This method first checks for a direct match in the root of {@code PERM_FILES_DIR} and then performs a recursive walk.
 *
 * @param fileName The name of the file to search for.
 * @return A {@link File} object representing the found file, or {@code null} if the file is not found
 *         or an {@link IOException} occurs during the directory traversal.
 */
    private File findFileRecursive(String fileName) {
        File root = new File(PERM_FILES_DIR);
        if (!root.exists()) return null;

        // Check Direct Path
        File direct = new File(root, fileName);
        if (direct.exists()) return direct;

        // Recursive Search
        try (java.util.stream.Stream<java.nio.file.Path> walk = Files.walk(root.toPath())) {
            return walk.filter(p -> !Files.isDirectory(p))
                    .filter(p -> p.getFileName().toString().equals(fileName))
                    .findFirst()
                    .map(java.nio.file.Path::toFile)
                    .orElse(null);
        } catch (IOException e) {
            System.err.println("Error searching perm_files: " + e.getMessage());
            return null;
        }
    }

    /**
 * Shuts down the {@code SchedulerServer} gracefully.
 * This method sets a flag to terminate the main server listening loop, shuts down the internal thread pool,
 * and attempts to close the {@link java.net.ServerSocket} to release the bound port.
 * Any {@link IOException} encountered during socket closure is caught and printed to the error stream.
 */
    public void stop(){
        isRunning = false;
        threadPool.shutdown();

        try{
            if(serverSocket != null && !serverSocket.isClosed())
                serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

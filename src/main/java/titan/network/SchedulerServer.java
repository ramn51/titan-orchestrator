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
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.FileAlreadyExistsException;
import java.util.*;
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
    private final StatsQueryRouter statsQueries;
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
        this.statsQueries = new StatsQueryRouter(scheduler);
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

                if (isErrorResponse(packet.opCode, responsePayload)) {
                    responseOpCode = TitanProtocol.OP_ERROR;
                }
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

    /** Data replies can contain arbitrary user text, so classify only command failures. */
    private static boolean isErrorResponse(byte requestOpCode, String response) {
        if (response == null) return true;
        if (requestOpCode == TitanProtocol.OP_KV_GET
                || requestOpCode == TitanProtocol.OP_KV_SMEMBERS
                || requestOpCode == TitanProtocol.OP_STATS
                || requestOpCode == TitanProtocol.OP_GET_JOB_STATUS) return false;
        if (requestOpCode == TitanProtocol.OP_GET_LOGS) {
            return response.equals("ERROR: Invalid job ID")
                    || response.equals("ERROR: Log file exists but unreadable");
        }
        return response.startsWith("ERROR") || response.startsWith("UNKNOWN")
                || response.startsWith("UPLOAD_FAILED")
                || response.startsWith("COMMUNICATION_ERROR")
                || response.startsWith("SERVER_ERROR");
    }

    private static boolean isSafeFileName(String name) {
        if (name == null || name.isBlank() || name.length() > 255
                || name.equals(".") || name.equals("..")
                || !name.equals(name.trim()) || name.endsWith(".")
                || name.indexOf('/') >= 0 || name.indexOf('\\') >= 0
                || name.indexOf(':') >= 0 || name.indexOf('|') >= 0) return false;
        return name.chars().noneMatch(c -> c < 32 || c == 127);
    }

    /** Resolve one filename beneath a root, rejecting symlinks that point outside it. */
    private static Path resolveSafeFile(String directory, String name) throws IOException {
        if (!isSafeFileName(name)) throw new IllegalArgumentException("Invalid filename");
        Path root = Path.of(directory).toAbsolutePath().normalize();
        Path candidate = root.resolve(name).normalize();
        if (!candidate.startsWith(root)) throw new IllegalArgumentException("Invalid filename");
        if (Files.exists(root)) {
            Path realRoot = root.toRealPath();
            if (Files.exists(candidate, LinkOption.NOFOLLOW_LINKS)
                    && !candidate.toRealPath().startsWith(realRoot)) {
                throw new IllegalArgumentException("Filename escapes directory");
            }
        }
        return candidate;
    }

    /**
 * Processes a request to deploy a file (e.g., a JAR or script) to a worker.
 * It reads the specified file from {@code uploads} or {@code perm_files}, encodes its content in Base64,
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
            if (!isSafeFileName(fileName)) return "ERROR: Invalid filename";
            if ((port == null || port.isBlank()) && fileName.equalsIgnoreCase("Worker.jar")) port = "8085";
            if (port == null || port.isBlank()) return "ERROR: A listening port is required";
            try {
                int parsedPort = Integer.parseInt(port);
                if (parsedPort < 1 || parsedPort > 65535) return "ERROR: Invalid listening port";
            } catch (NumberFormatException error) {
                return "ERROR: Invalid listening port";
            }
            Path file = resolveSafeFile("uploads", fileName);
            if (!Files.isRegularFile(file)) file = resolveSafeFile(PERM_FILES_DIR, fileName);
            if (!Files.isRegularFile(file)) {
                return "ERROR: File not found in " + PERM_FILES_DIR;
            }

            byte[] fileBytes = Files.readAllBytes(file);
            String base64Content = Base64.getEncoder().encodeToString(fileBytes);

            Job job = new Job("TEMP_PAYLOAD", 1, 0);
            String internalId = job.getId();

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

        } catch (IOException | IllegalArgumentException e) {
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
            if (!isSafeFileName(fileName)) return "ERROR: Invalid filename";
            String safeReq = (requirement == null || requirement.isEmpty()) ? "GENERAL" : requirement;
            System.out.println("[INFO] Looking for '" + fileName + "' in workspace...");
            File file = findFileRecursive(fileName);
            if (file == null) {
                Path uploaded = resolveSafeFile("uploads", fileName);
                if (Files.isRegularFile(uploaded)) file = uploaded.toFile();
            }

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
        } catch (IOException | IllegalArgumentException e) {
            e.printStackTrace();
            return "ERROR: Server File IO issue";
        }
    }

    private boolean detectCycle(List<Job> dagJobs){
        Map<String, List<String>> adjList = new HashMap<>();
        Map<String, Integer> inDegrees = new HashMap<>();
        Queue<String> queue = new ArrayDeque<>();

        for(Job job: dagJobs){
            adjList.putIfAbsent(job.getId(), new ArrayList<>());
            inDegrees.putIfAbsent(job.getId(), 0);

            for(String parentId : job.getDependenciesIds()){
                if (!adjList.containsKey(parentId) && dagJobs.stream().noneMatch(j -> j.getId().equals(parentId))) {
                    continue;
                }

                adjList.putIfAbsent(parentId, new ArrayList<>());
                inDegrees.putIfAbsent(parentId, 0);

                adjList.get(parentId).add(job.getId());
                inDegrees.put(job.getId(), inDegrees.get(job.getId()) + 1);

            }
        }

        for (Map.Entry<String, Integer> entry : inDegrees.entrySet()) {
            if (entry.getValue() == 0) {
                queue.offer(entry.getKey());
            }
        }

        int processed = 0;

        while(!queue.isEmpty()){
            String curr = queue.poll();
            processed ++;

            for(String child : adjList.get(curr)){
                inDegrees.put(child, inDegrees.get(child) - 1);
                if (inDegrees.get(child) == 0) {
                    queue.offer(child);
                }
            }
        }

        return processed != inDegrees.size();
    }

    /**
 * Parses a string containing multiple job definitions for a Directed Acyclic Graph (DAG) and submits each job to the {@link titan.scheduler.Scheduler}.
 * Each job definition within the request string is expected to be separated by a semicolon ({@code ;}).
 * Jobs are parsed using {@link titan.scheduler.Job#fromDagString(String)}.
 *
 * @param request A string containing one or more job definitions, typically in a format like "job1_def;job2_def;..."
 *                where each definition can be parsed into a {@link titan.scheduler.Job} object.
 */
    private String parseAndSubmitDAG(String request){
        String [] jobs = request.split(";");
        List<Job> parsedDagJobs = new ArrayList<>();
        for(String jobDef: jobs){
            if(jobDef.trim().isEmpty()) continue;
            try {
                Job job = Job.fromDagString(jobDef.trim());
                parsedDagJobs.add(job);
            } catch (Exception e) {
                System.err.println("[FAIL] Failed to parse DAG job: " + jobDef + " Error: " + e.getMessage());
                return "ERROR: Invalid DAG job: " + e.getMessage();
            }
        }

        if (parsedDagJobs.isEmpty()) return "ERROR: Empty DAG";

        if(detectCycle(parsedDagJobs)){
            System.err.println("[FATAL] Cycle detected in DAG! Rejecting entire payload to prevent deadlock.");
            return "ERROR: DAG contains a cycle";
        }

        scheduler.submitDag(parsedDagJobs);

        return "DAG_ACCEPTED";

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
                return statsQueries.execute(payload);

            case TitanProtocol.OP_CLEAN_STATS:
                scheduler.clearRuntimeStats();
                return "Stats Map Cleared. Run STATS again to see fresh state.";

            case TitanProtocol.OP_UNREGISTER_SERVICE:
                String serviceId = payload;
                scheduler.unregisterService(serviceId);
                System.out.println("[INFO] Cleaned up service: " + serviceId);
                return "ACK_UNREGISTERED";

            case TitanProtocol.OP_STOP:
                if (payload.isEmpty()) return "ERROR: Missing Service ID";
                return scheduler.stopRemoteService(payload);

            case TitanProtocol.OP_RESOLVE_SERVICE:
                if (payload.isBlank()) return "ERROR: Missing Service ID";
                return scheduler.resolveService(payload.trim());

            case TitanProtocol.OP_STATS:
                return scheduler.getSystemStats();

            case TitanProtocol.OP_SUBMIT_DAG:
                return parseAndSubmitDAG(payload);

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
                    if (!isSafeFileName(batchJobId)) return "ERROR: Invalid job ID";
                    String fullLogBlock = batchParts[1];

                    // Split the block back into individual lines
                    // The Worker joined them with "\n", so we split by "\n"
                    String[] batchLines = fullLogBlock.split("\n");
                    for (String line : batchLines) {
                        scheduler.logStream(batchJobId, line);
                    }
                } else return "ERROR: Invalid log batch";
                return "ACK_BATCH";

            case TitanProtocol.OP_LOG_STREAM:
                // Payload format: "jobId|logLine"
                // Split into 2 parts max so the log message can contain pipes safely
                String[] logParts = payload.split("\\|", 2);
                if (logParts.length == 2) {
                    if (!isSafeFileName(logParts[0])) return "ERROR: Invalid job ID";
                    scheduler.logStream(logParts[0], logParts[1]);
                } else return "ERROR: Invalid log line";
                return "ACK_LOG"; // send back an ack

            case TitanProtocol.OP_GET_LOGS:
                // Payload is the jobId in this case
                String jobId = payload;
                if (!isSafeFileName(jobId)) return "ERROR: Invalid job ID";
                List<String> logs = scheduler.getLogs(jobId);
                if (logs.isEmpty()) {
                    try {
                        Path logFile = resolveSafeFile("titan_server_logs", jobId + ".log");
                        if (Files.isRegularFile(logFile)) {
                            return new String(Files.readAllBytes(logFile));
                        }
                    } catch (IOException e) {
                        return "ERROR: Log file exists but unreadable";
                    } catch (IllegalArgumentException e) {
                        return "ERROR: Invalid job ID";
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
                    if (!isSafeFileName(assetName))
                        return "ERROR: Invalid asset name";
                    Files.createDirectories(Path.of("uploads"));

                    byte[] decodedBytes = Base64.getDecoder().decode(assetData);
                    Path destFile = resolveSafeFile("uploads", assetName);
                    if (Files.isSymbolicLink(destFile)) return "ERROR: Invalid asset name";
                    Files.write(destFile, decodedBytes);
                    System.out.println("[UPLOAD] Saved asset: " + assetName + " (" + decodedBytes.length + " bytes)");
                    return "UPLOAD_SUCCESS";
                }catch (Exception e) {
                    e.printStackTrace();
                    return "UPLOAD_FAILED: " + e.getMessage();
                }

            case TitanProtocol.OP_FETCH_ASSET:
                // Payload: "filename" (e.g., "my_project.zip")
                String requestedFile = payload.trim();
                if (!isSafeFileName(requestedFile))
                    return "ERROR: Invalid asset name";
                try {
                    Path assetFile = resolveSafeFile("uploads", requestedFile);
                    if (!Files.isRegularFile(assetFile)) return "ERROR_NOT_FOUND";
                    byte[] fileBytes = Files.readAllBytes(assetFile);
                    return Base64.getEncoder().encodeToString(fileBytes);
                } catch (IllegalArgumentException e) {
                    return "ERROR: Invalid asset name";
                } catch (IOException e) {
                    return "ERROR_READING_FILE: " + e.getMessage();
                }

            case TitanProtocol.OP_DEPLOY_SCRIPT:
                String[] scriptParts = payload.split("\\|", 2);
                if (scriptParts.length != 2 || scriptParts[1].isBlank())
                    return "ERROR: Expected filename|Base64 script content";
                String scriptName = scriptParts[0];
                if (!scriptName.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
                        || scriptName.equals(".") || scriptName.equals(".."))
                    return "ERROR: Invalid script filename";
                if (scriptParts[1].length() > 6_000_000)
                    return "ERROR: Script exceeds 4 MB limit";
                try {
                    byte[] scriptBytes = Base64.getDecoder().decode(scriptParts[1]);
                    if (scriptBytes.length > 4_000_000) return "ERROR: Script exceeds 4 MB limit";
                    Path scriptDir = Path.of(PERM_FILES_DIR);
                    Files.createDirectories(scriptDir);
                    Path scriptPath = resolveSafeFile(PERM_FILES_DIR, scriptName);
                    if (Files.isSymbolicLink(scriptPath)) return "ERROR: Invalid script filename";
                    Files.write(scriptPath, scriptBytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                    return "SCRIPT_STAGED:" + scriptName;
                } catch (IllegalArgumentException error) {
                    return "ERROR: Invalid Base64 script content";
                } catch (FileAlreadyExistsException error) {
                    return "ERROR: Script already exists";
                } catch (IOException error) {
                    return "ERROR: Could not stage script: " + error.getMessage();
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

            case TitanProtocol.OP_CANCEL_JOB:
                return scheduler.cancelJob(payload.trim());


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
        if (!isSafeFileName(fileName)) return null;
        Path root = Path.of(PERM_FILES_DIR);
        if (!Files.isDirectory(root)) return null;

        // Check Direct Path
        try {
            Path direct = resolveSafeFile(PERM_FILES_DIR, fileName);
            if (Files.isRegularFile(direct)) return direct.toFile();
        } catch (IOException | IllegalArgumentException e) {
            return null;
        }

        // Recursive Search
        try (java.util.stream.Stream<Path> walk = Files.walk(root)) {
            Path realRoot = root.toRealPath();
            return walk.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().equals(fileName))
                    .filter(p -> {
                        try { return p.toRealPath().startsWith(realRoot); }
                        catch (IOException e) { return false; }
                    })
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

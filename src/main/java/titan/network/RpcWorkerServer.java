/*
 * Copyright 2026 Ram Narayanan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */

package titan.network;

import titan.filesys.WorkspaceManager;
import titan.tasks.TaskHandler;
import titan.tasks.FileHandler;
import titan.tasks.PdfConversionHandler;
import titan.tasks.ScriptExecutorHandler;
import titan.tasks.ServiceHandler;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a worker server in the Titan distributed system. This server listens for incoming RPC
 * requests from a scheduler, executes various tasks (e.g., file staging, script execution, service
 * management, PDF conversion), and reports job status and logs back to the scheduler.
 * <p>
 * Each worker server registers itself with a central scheduler, advertising its capabilities. It uses
 * a thread pool to handle incoming client connections and a separate worker pool to manage the
 * execution of tasks, ensuring that the server remains responsive while processing jobs.
 * </p>
 */
public class RpcWorkerServer {

    // The port number on which this worker server listens for incoming connections.
    private int port;

    // A thread pool used to handle incoming client connections. Each new client connection is submitted to this pool for processing.
    private final ExecutorService threadPool;

    /**
     * A volatile flag indicating whether the worker server is currently running. Set to {@code false} to initiate a graceful shutdown.
     */
    private volatile boolean isRunning = true;

    /**
     * A string describing the capabilities of this worker, used by the scheduler to assign appropriate tasks. Examples include "GENERAL", "GPU", "PDF_CONVERT", etc.
     */
    private String capability;

    /**
     * A flag indicating whether this worker is a permanent instance ({@code true}) or an ephemeral instance ({@code false}) that can be scaled down by the scheduler.
     */
    private boolean isPermanent;

    /**
     * The port number of the scheduler server to which this worker registers and sends callbacks.
     */
    private int schedulerPort;

    /**
     * The hostname or IP address of the scheduler server to which this worker registers and sends callbacks.
     */
    private String schedulerHost;

    /**
     * A map storing various {@link titan.tasks.TaskHandler} implementations, keyed by their task type. This allows the worker to dynamically dispatch tasks based on the received command.
     */
    private Map<String, TaskHandler> taskHanlderMap;

    /**
     * The maximum number of concurrent tasks that this worker can execute simultaneously in its worker pool.
     */
    private static final int MAX_THREADS = 4;

    /**
     * A fixed-size thread pool dedicated to executing actual tasks (jobs). This limits the concurrent workload on the worker.
     */
    private final ExecutorService workerPool;

    /**
     * An atomic counter tracking the number of jobs currently being processed by the {@code workerPool}. Used to manage worker saturation.
     */
    private final AtomicInteger activeJobs;

    /**
     * Constructs a new {@code RpcWorkerServer} instance.
     *
     * @param myPort The port number on which this worker server will listen.
     * @param schedulerHost The hostname or IP address of the scheduler.
     * @param schedulerPort The port number of the scheduler.
     * @param capability A string describing the capabilities of this worker (e.g., "GENERAL", "PDF_CONVERT").
     * @param isPermanent A boolean indicating if this worker is a permanent instance (true) or ephemeral (false).
     */
    public RpcWorkerServer( int myPort, String schedulerHost, int schedulerPort, String capability, boolean isPermanent){
        this.port = myPort;
        this.threadPool = Executors.newCachedThreadPool();
        this.capability = capability;
        this.schedulerHost = schedulerHost;
        this.schedulerPort = schedulerPort;
        this.taskHanlderMap = new HashMap<>();
        this.isPermanent = isPermanent;

        workerPool = Executors.newFixedThreadPool(MAX_THREADS);
        activeJobs =  new AtomicInteger(0);

        addTaskHandler();
    }

    /**
     * Retrieves the hostname of the scheduler this worker is connected to.
     *
     * @return The scheduler's hostname.
     */
    public String getSchedulerHost() { return schedulerHost; }

    /**
     * Retrieves the port number of the scheduler this worker is connected to.
     *
     * @return The scheduler's port number.
     */
    public int getSchedulerPort() { return schedulerPort; }

    /**
     * Initializes and registers various {@link titan.tasks.TaskHandler} implementations with the worker server. This method populates the {@code taskHanlderMap} with handlers for specific task types like PDF conversion, file staging, service management, and script execution.
     */
    public void addTaskHandler(){
        taskHanlderMap.put("PDF_CONVERT", new PdfConversionHandler());
        taskHanlderMap.put("STAGE_FILE", new FileHandler());
        taskHanlderMap.put("START_SERVICE", new ServiceHandler("START", this));
        taskHanlderMap.put("STOP_SERVICE", new ServiceHandler("STOP", this));
        taskHanlderMap.put("RUN_SCRIPT", new ScriptExecutorHandler(this));

        taskHanlderMap.put("DEPLOY_PAYLOAD", new FileHandler());

        // This is for Shutting down the worker
//        taskHanlderMap.put("SHUTDOWN_WORKER", (payload) -> {
//            new Thread(() -> {
//                try { Thread.sleep(1000); } catch (Exception e) {}
//                System.out.println("Received SHUTDOWN command. Exiting...");
//                System.exit(0);
//            }).start();
//            return "ACK_SHUTTING_DOWN";
//        });
    }

    /**
     * Starts the worker server, binding it to the specified port and beginning to listen for incoming client connections. It also registers itself with the scheduler upon startup.
     * <p>
     * This method enters a continuous loop, accepting new client sockets and submitting them to the internal thread pool for handling. The loop continues as long as the {@code isRunning} flag is {@code true}.
     * </p>
     *
     * @throws Exception If an error occurs during server startup or socket operations.
     */
    public void start() throws Exception {
        System.out.println("DEBUG: Attempting to bind to port: " + this.port);
        System.out.println("---- Worker Startup Check ----");
        titan.tasks.ProcessRegistry.loadAndCleanUpProcesses();

        try(ServerSocket serverSocket = new ServerSocket(port)){
            System.out.println("Worker Server started on port " + port);
            registerWithScheduler();

            while(this.isRunning){
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(() -> clientHandler(clientSocket));
            }

        } catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Registers this worker server with the central scheduler. It sends its port, capabilities, and permanence status to the scheduler and waits for a registration confirmation.
     *
     * @throws Exception If an I/O error occurs during communication with the scheduler or if registration fails.
     */
    private void registerWithScheduler() throws Exception {
        try(Socket socket = new Socket(schedulerHost, schedulerPort);
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream())){
            String requestPayload = port + "||" + capability + "||" + this.isPermanent;
            TitanProtocol.send(out, TitanProtocol.OP_REGISTER, requestPayload);
            TitanProtocol.TitanPacket responsePacket = TitanProtocol.read(in);
            if ("REGISTERED".equals(responsePacket.payload)) {
                System.out.println("[OK] Successfully registered with Scheduler!");
            } else {
                System.err.println("[FAIL] Registration failed: " + responsePacket.payload);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * Handles communication with a single client socket (typically the scheduler). This method continuously reads {@link titan.network.TitanProtocol.TitanPacket}s from the client, processes them based on their operation code, and sends appropriate responses.
     * <p>
     * Supported operations include heartbeats, running archive jobs, starting archived services, staging files, starting/stopping services, running scripts, and worker shutdown commands.
     * </p>
     *
     * @param socket The client socket connected to the worker.
     */
    private void clientHandler(Socket socket){
        try(socket; DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        ){
            while (!socket.isClosed()) {
                try {
                    TitanProtocol.TitanPacket packet = TitanProtocol.read(in);

                    if (packet.opCode == TitanProtocol.OP_HEARTBEAT) {

                        int activeThreads = 0;
                        int maxThreads = 4;

                        if(this.workerPool !=null){
                            if (this.workerPool instanceof java.util.concurrent.ThreadPoolExecutor) {
                                activeThreads = ((java.util.concurrent.ThreadPoolExecutor) this.workerPool).getActiveCount();
                            } else{
                                activeThreads = activeJobs.get();
                            }
                        }

                        String stats = "PONG|" + activeThreads + "|" + maxThreads;
                        TitanProtocol.send(out, TitanProtocol.OP_ACK, stats);

                    } else if(packet.opCode == TitanProtocol.OP_RUN_ARCHIVE){
                        handleArchiveJob(out, packet.payload);

                    } else if(packet.opCode == TitanProtocol.OP_START_SERVICE_ARCHIVE){
                        handleArchiveService(out, packet.payload);

                    } else if (packet.opCode == TitanProtocol.OP_STAGE) {
                        // handleExecution(out, packet.payload, "STAGE_FILE");
                        handleSyncExecution(out, packet.payload, "STAGE_FILE");
                    } else if (packet.opCode == TitanProtocol.OP_START_SERVICE) {
//                        handleExecution(out, packet.payload, "START_SERVICE");
                        handleSyncExecution(out, packet.payload, "START_SERVICE");
                    } else if (packet.opCode == TitanProtocol.OP_STOP) {
//                        handleExecution(out, packet.payload, "STOP_SERVICE");
                        handleSyncExecution(out, packet.payload, "STOP_SERVICE");
                    } else if (packet.opCode == TitanProtocol.OP_RUN) {
//                        if (packet.payload.startsWith("SHUTDOWN_WORKER")) {
//                            System.out.println("Worker received kill signal. Shutting down...");
//                            // Send confirmation back before dying
//                            TitanProtocol.send(out, TitanProtocol.OP_RUN, "SUCCESS: Worker shutting down.");
//                            Thread.sleep(100); // add busy waiting for the OS to handle the exit
//                            System.exit(0);
//                        }
                        handleAsyncExecution(out, packet.payload);
                    } else if (packet.opCode == TitanProtocol.OP_KILL_WORKER) {
                        System.out.println("[INFO] Worker received Kill Signal (OP_KILL_WORKER). Shutting down...");
                        TitanProtocol.send(out, TitanProtocol.OP_KILL_WORKER, "SUCCESS: Worker shutting down.");
                        // add busy waiting for the OS to handle the exit
                        try { Thread.sleep(100); } catch (InterruptedException ignored) {}
                        // Kill the JVM
                        System.exit(0);
                    }
                } catch (EOFException e) {
//                    System.out.println("Scheduler disconnected.");
                    break;
                } catch (Exception e) {
                    System.err.println("Error processing packet: " + e.getMessage());
                    break;
                }
            }
        } catch (IOException e){
            System.err.println("Connection error in clientHandler: " + e.getMessage());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Handles the execution of a task, either synchronously or asynchronously depending on the worker's saturation. If the worker pool is saturated, an error is returned. Otherwise, the task is submitted to the worker pool.
     * <p>
     * This method is deprecated in favor of {@link #handleAsyncExecution(DataOutputStream, String)} and {@link #handleSyncExecution(DataOutputStream, String, String)} for clearer separation of concerns.
     * </p>
     *
     * @param out The output stream to send responses back to the client.
     * @param payload The task-specific data required for execution.
     * @param forceTaskType An optional parameter to explicitly specify the task type, overriding parsing from the payload.
     */
    private void handleExecution(DataOutputStream out, String payload, String forceTaskType) {
        if(activeJobs.get() >= MAX_THREADS){
            try {
                TitanProtocol.send(out, TitanProtocol.OP_ERROR, "ERROR_WORKER_SATURATED");
            } catch (IOException e) { e.printStackTrace(); }
        } else {
            activeJobs.incrementAndGet();
            try {
                workerPool.submit(() -> {
                    try {
                        // If forceTaskType is set (for DEPLOY), use it. Otherwise parse from payload.
                        String response = (forceTaskType != null)
                                ? processCommandExplicit(forceTaskType, payload)
                                : processCommand(payload);

                        byte status = response.startsWith("ERROR") || response.contains("FAILED")
                                ? TitanProtocol.OP_ERROR
                                : TitanProtocol.OP_ACK;

                        synchronized (out) {
                            try {
                                TitanProtocol.send(out, status, response);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Worker execution error: " + e.getMessage());
                    } finally {
                        activeJobs.decrementAndGet();
                    }
                }).get();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    /**
     * Handles the execution of a job that involves staging and running an archived payload. The payload is expected to contain a job ID, an entry file path, and a Base64 encoded ZIP archive.
     * <p>
     * This method first stages the archive using {@link titan.filesys.WorkspaceManager}, resolves the entry file path, and then delegates to {@link #handleAsyncExecution(DataOutputStream, String)} to run the job asynchronously.
     * </p>
     *
     * @param out The output stream to send responses back to the client.
     * @param payload The payload containing job ID, entry file, and Base64 ZIP data (format: "JOB_ID|ENTRY_FILE|BASE64_ZIP").
     */
    private void handleArchiveJob(DataOutputStream out, String payload){
        // Payload: JOB_ID | ENTRY_FILE | BASE64_ZIP
        try{
            String[] parts = payload.split("\\|");
            String jobId = parts[0];
            String entryFile = parts[1];
            String base64Zip = parts[2];

            WorkspaceManager.stageArchive(jobId, base64Zip);
            String absPath = WorkspaceManager.resolvePath(jobId, entryFile);

            handleAsyncExecution(out, jobId + "|" + absPath);
        } catch(Exception e){
            e.printStackTrace();
            try { TitanProtocol.send(out, TitanProtocol.OP_ERROR, "ARCHIVE_FAILED: " + e.getMessage()); } catch (IOException ignored) {}
        }
    }

    /**
     * Handles the deployment and startup of a service from an archived payload. The payload is expected to contain a service ID, an entry file path, a port, and a Base64 encoded ZIP archive.
     * <p>
     * This method stages the archive, resolves the entry file, and then delegates to {@link #handleSyncExecution(DataOutputStream, String, String)} to start the service synchronously using the "START_SERVICE" handler.
     * </p>
     *
     * @param out The output stream to send responses back to the client.
     * @param payload The payload containing service ID, entry file, port, and Base64 ZIP data (format: "SERVICE_ID|ENTRY_FILE|PORT|BASE64_ZIP").
     */
    private void handleArchiveService(DataOutputStream out, String payload){
        try{
            // Payload: SERVICE_ID | ENTRY_FILE | PORT | BASE64_ZIP
            String[] parts = payload.split("\\|");
            String serviceId = parts[0];
            String entryFile = parts[1];
            String port = parts[2];
            String base64Zip = parts[3];

            WorkspaceManager.stageArchive(serviceId, base64Zip);
            String absPath = WorkspaceManager.resolvePath(serviceId, entryFile);
//            Payload expected by ServiceHandler: "FILENAME | SERVICE_ID | PORT"
            String handlerPayload = absPath + "|" + serviceId + "|" + port;
            handleSyncExecution(out, handlerPayload, "START_SERVICE");
        } catch (Exception e){
            e.printStackTrace();
            try { TitanProtocol.send(out, TitanProtocol.OP_ERROR, "ARCHIVE_SERVICE_FAILED: " + e.getMessage()); } catch (IOException ignored) {}
        }
    }

    /**
     * Processes a command by explicitly using a provided task type and task data. This method looks up the appropriate {@link titan.tasks.TaskHandler} from the {@code taskHanlderMap} and executes it.
     *
     * @param taskType The explicit type of the task to execute (e.g., "PDF_CONVERT", "START_SERVICE").
     * @param taskData The data payload for the task handler.
     * @return A string representing the result of the task execution, or an error message if the task type is unknown or execution fails.
     */
    private String processCommandExplicit(String taskType, String taskData) {
        TaskHandler handler = taskHanlderMap.get(taskType);
        if(handler!=null){
            try{
                return handler.execute(taskData);
            } catch (Exception e){
                return "JOB_FAILED " + e.getMessage();
            }
        }
        return "ERROR: Unknown TaskType " + taskType;
    }

    /**
     * Notifies the scheduler (master) that a specific service has stopped on this worker. This is typically called by a {@link titan.tasks.ServiceHandler} when a managed service terminates.
     *
     * @param serviceId The unique identifier of the service that has stopped.
     */
    public void notifyMasterOfServiceStop(String serviceId) {
        // masterHost and masterPort should be variables in your RpcWorkerServer class
        try (Socket socket = new Socket(this.schedulerHost, this.schedulerPort);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {
            TitanProtocol.send(out, TitanProtocol.OP_UNREGISTER_SERVICE, serviceId);
            System.out.println("[TitanProtocol] Sent UNREGISTER_SERVICE for " + serviceId);

        } catch (IOException e) {
            System.err.println("[FAIL] Failed to notify Master: " + e.getMessage());
        }
    }

    /**
     * Parses a command payload to determine the task type and data, then dispatches it to the appropriate {@link titan.tasks.TaskHandler}. The payload is expected to be in the format "TASK_TYPE|ARGS...".
     * <p>
     * This method also includes logic for simulated test commands like "TEST" and "FAIL", and a sleep command for debugging.
     * </p>
     *
     * @param payload The full command string received from the client.
     * @return A string representing the result of the task execution, or an error message if the format is invalid, task type is unknown, or execution fails.
     */
    private String processCommand(String payload){
        // Payload format: "TASK_TYPE|ARGS..."
        String[] parts = payload.split("\\|", 2);
        if(parts.length < 2)
            return "INVALID_JOB_FORMAT";

        // Ex: "START_SERVICE" or "PDF_CONVERT"
        String taskType = parts[0];
        // Ex: "file.jar|jobId|8085"
        String taskData = parts[1];

        if (payload.contains("SLEEP")) {
            try { Thread.sleep(5000); } catch (Exception e) {}
        }

        if (taskType.equals("TEST")) {
            return "SUCCESS_PROCESSED_" + payload;
        }

        if (payload.contains("FAIL")) {
            return "JOB_FAILED_SIMULATED_ERROR";
        }

        TaskHandler handler = taskHanlderMap.get(taskType);
        if(handler!=null){
            try{
                return handler.execute(taskData);
            } catch (Exception e){
                return "JOB_FAILED" + e.getMessage();
            }
        } else{
            return "ERROR: Task doesnt exist so I dont know how to do " + taskType;
        }
    }

    /**
     * Handles the asynchronous execution of a task. If the worker pool is saturated, an error is immediately sent back. Otherwise, the task is submitted to the {@code workerPool} and an "JOB_ACCEPTED" acknowledgment is sent.
     * <p>
     * The actual task execution happens in a separate thread. Upon completion (success or failure), a callback is sent to the scheduler using {@link #sendCallback(String, String, String)}.
     * </p>
     *
     * @param out The output stream to send the immediate acknowledgment back to the client.
     * @param payload The task-specific data, potentially including a job ID (format: "JOB_ID|TASK_DATA" or just "TASK_DATA").
     */
    private void handleAsyncExecution(DataOutputStream out, String payload){
        if(activeJobs.get() >= MAX_THREADS){
            try{
                TitanProtocol.send(out, TitanProtocol.OP_ERROR, "ERROR_WORKER_SATURATED");
            } catch (IOException e){
                e.printStackTrace();
            }
            return;
        }

        activeJobs.incrementAndGet();
        workerPool.submit(() ->{
            // Parse Job ID for Callback
            // Payload Format expected: "JOB-123|calc.py" (Standard) or "calc.py" (Legacy/Direct)
            String[] parts = payload.split("\\|", 2);
            String jobId = (parts.length > 1) ? parts[0] : "UNKNOWN";
            String taskData = (parts.length > 1) ? parts[1] : payload;
            // This for Test jobs
            if(parts[0].equals("TEST")) { jobId = "TEST-JOB"; taskData = parts[1]; }
            try {
                System.out.println("[ASYNC] Starting job "+ jobId + ": " + taskData);
                TaskHandler handler = taskHanlderMap.get("RUN_SCRIPT");

                // RE-INJECT ID for the Handler
                // We combine them so ScriptExecutorHandler gets "calc.py | JOB-123"
//                String payloadForHandler = taskData + "|" + jobId;

                // Result: "JOB-123|my_script.py|--verbose"
                String payloadForHandler = jobId + "|" + taskData;
                String result = handler.execute(payloadForHandler);
                // This allows the worker to handle PDF_CONVERT or any other key
//                String result = processCommand(taskData);

                System.out.println("[ASYNC] Finished "+ jobId);
                sendCallback(jobId, "COMPLETED", result);
            } catch(Exception e){
                System.err.println("[ASYNC] Failed " + jobId + ": " + e.getMessage());
                sendCallback(jobId, "FAILED", e.getMessage());
            } finally {
                activeJobs.decrementAndGet();
            }
        });
        try {
            TitanProtocol.send(out, TitanProtocol.OP_ACK, "JOB_ACCEPTED");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Sends a job completion or failure callback to the scheduler. This method establishes a new socket connection to the scheduler to report the job's final status and result.
     * <p>
     * Callbacks are not sent for "UNKNOWN" or "TEST-JOB" job IDs.
     * </p>
     *
     * @param jobId The unique identifier of the job.
     * @param status The final status of the job (e.g., "COMPLETED", "FAILED").
     * @param result A string containing the output or error message from the job execution.
     */
    private void sendCallback(String jobId, String status, String result) {
        if(jobId.equals("UNKNOWN") || jobId.equals("TEST-JOB")) return;

        try (Socket socket = new Socket(schedulerHost, schedulerPort);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            String msg = jobId + "|" + status + "|" + result;
            TitanProtocol.send(out, TitanProtocol.OP_JOB_COMPLETE, msg);
            TitanProtocol.read(in);

        } catch (Exception e) {
            System.err.println("[FAIL] Callback failed: " + e.getMessage());
        }
    }

    /**
     * Handles the synchronous execution of a task. The task is processed immediately using {@link #processCommandExplicit(String, String)}, and the result (ACK or ERROR) is sent back to the client before the method returns.
     *
     * @param out The output stream to send the response back to the client.
     * @param payload The task-specific data for execution.
     * @param forceTaskType The explicit type of the task to execute.
     */
    private void handleSyncExecution(DataOutputStream out, String payload, String forceTaskType){
        try{
            String response = processCommandExplicit(forceTaskType, payload);
            byte status = response.startsWith("ERROR") || response.contains("FAILED")
                    ? TitanProtocol.OP_ERROR
                    : TitanProtocol.OP_ACK;
            TitanProtocol.send(out, status, response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Streams a single log line for a given job to the scheduler (master). This method establishes a new socket connection to send the log data.
     * <p>
     * Errors during log streaming are silently ignored to prevent disrupting the main worker flow.
     * </p>
     *
     * @param jobId The unique identifier of the job to which the log line belongs.
     * @param line The log message to stream.
     */
    public void streamLogToMaster(String jobId, String line){
        try (Socket socket = new Socket(this.schedulerHost, this.schedulerPort);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            TitanProtocol.send(out, TitanProtocol.OP_LOG_STREAM, jobId + "|" + line);
        } catch (Exception e) { /* ignore */ }
    }

    /**
     * Initiates a graceful shutdown of the worker server. It sets the {@code isRunning} flag to {@code false} to stop the main server loop and then shuts down the internal thread pool, preventing new tasks from being accepted.
     */
    public void stop(){
        isRunning = false;
        threadPool.shutdown();
    }

    /**
     * The main entry point for the RpcWorkerServer application. This method parses command-line arguments to configure the worker's port, scheduler host/port, capabilities, and permanence status.
     * <p>
     * It then initializes and starts an {@code RpcWorkerServer} instance.
     * </p>
     *
     * @param args Command-line arguments: [myPort] [schedulerHost] [schedulerPort] [capability] [isPermanent].
     * @throws Exception If an error occurs during server initialization or startup.
     */
    public static void main(String[] args) throws Exception {
        int myPort = 8080;
        String schedHost = "localhost";
        int schedPort = 9090;
        String capability = "GENERAL";
        boolean isPermanent = false;

        // 2. Parse Arguments (Order: port, schedHost, schedPort, capability, isPermanent)
        if (args.length > 0) {
            try {
                myPort = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port provided, using default 8080");
            }
        }

        if (args.length > 1) schedHost = args[1];

        if (args.length > 2) {
            try {
                schedPort = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid  port, using default 9090");
            }
        }

        if (args.length > 3 && args[3] != null && !args[3].trim().isEmpty()) {
            String arg3 = args[3].trim();
            if (arg3.equalsIgnoreCase("true") || arg3.equalsIgnoreCase("false")) {
                System.out.println("Detected boolean in Capability slot. Auto-correcting...");
                capability = "GENERAL";
                isPermanent = Boolean.parseBoolean(arg3);
            } else {
                capability = arg3;
                if (args.length > 4) {
                    isPermanent = Boolean.parseBoolean(args[4]);
                }
            }
        }

        System.out.println("Starting Worker Server...");
        System.out.println("Local Port: " + myPort);
        System.out.println("Target Scheduler: " + schedHost + ":" + schedPort);
        System.out.println("Capability: " + capability);
        System.out.println("Mode:             " + (isPermanent ? "PERMANENT (Protected)" : "EPHEMERAL (Auto-Scaleable)"));

        titan.tasks.ProcessRegistry.loadAndCleanUpProcesses();

        RpcWorkerServer rpcWorkerServer = new RpcWorkerServer(myPort, schedHost, schedPort, capability, isPermanent);
        rpcWorkerServer.start();
    }
}
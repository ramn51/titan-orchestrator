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

package titan.tasks;

import titan.network.LogBatcher;
import titan.network.RpcWorkerServer;
import titan.tasks.TaskHandler;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@code ServiceHandler} is an implementation of {@link TaskHandler} responsible for managing the lifecycle
 * of services (processes) on a worker node. It supports starting and stopping various types of services,
 * including Java JARs, Python scripts, and other executables.
 * <p>
 * This handler maintains a registry of currently running services and provides mechanisms for
 * launching them in a detached manner, capturing their output, and streaming logs back to a master server.
 * It also handles the cleanup of resources upon service termination.
 */
    public class ServiceHandler implements TaskHandler {
    /**
 * A static, thread-safe map that stores references to currently running {@link Process} objects,
 * keyed by their unique service IDs. This allows for tracking and managing active services.
 */
    private static final Map<String, Process> runningServices = new ConcurrentHashMap<>();
    /**
 * The base directory where service files are expected to be located and where service-related
 * logs and temporary files are stored.
 */
    private static final String WORKSPACE_DIR = "./titan_workspace/";
    /**
 * The specific operation this handler instance is configured to perform (e.g., "START", "STOP").
 * This field determines which internal method ({@code startProcess} or {@code stopProcess}) will be invoked
 * by the {@link #execute(String)} method.
 */
    private final String operation;

    /**
 * A reference to the parent {@link RpcWorkerServer} instance. This is used to facilitate
 * communication back to the master server, such as streaming logs or notifying about service status changes.
 */
    private final RpcWorkerServer parentServer;

    /**
 * Constructs a new {@code ServiceHandler} with a specified operation and a reference to its parent RPC server.
 *
 * @param op The operation this handler will perform (e.g., "START", "STOP").
 * @param parentServer The {@link RpcWorkerServer} instance that created this handler, used for callbacks.
 */
    public ServiceHandler(String op, RpcWorkerServer parentServer){
        this.operation = op;
        this.parentServer = parentServer;
    }

    /**
 * Executes a service management task based on the handler's configured operation and the provided payload.
 * The payload is expected to be a pipe-separated string containing service details.
 * <p>
 * If the operation is "START", it parses the payload for filename, service ID, and port, then attempts to start a new process.
 * If the operation is anything else (implicitly "STOP"), it parses the payload for a service ID and attempts to stop the corresponding process.
 *</p>
 * @param payload A string containing service parameters, typically in the format "filename|serviceId|port" for START,
 *                or "serviceId" for STOP.
 * @return A string indicating the result of the operation, such as "DEPLOYED_SUCCESS", "STOPPED", or an "ERROR" message.
 */
    @Override
    public String execute(String payload) {
        String [] parts = payload.split("\\|");

        if(operation.equals("START")){
            String fileName = parts[0];
            String serviceId = (parts.length > 1) ? parts[1] : "svc_" + System.currentTimeMillis();
            String portToUse = (parts.length > 2) ? parts[2] : "8085";

            return startProcess(fileName, serviceId, portToUse);
        } else {
            String idToKill = (parts.length > 1) ? parts[1] : parts[0];
            return stopProcess(idToKill);
        }
    }
    /**
     * Initiates the launch of a new service process.
     * <p>
     * This method handles different types of service files:
     * </p>
     * <ul>
     * <li>If {@code fileName} is "Worker.jar", it launches it as a Java JAR using {@code java -jar} with OS-specific detachment.</li>
     * <li>If {@code fileName} ends with ".py", it launches it using the "python" interpreter.</li>
     * <li>For any other file, it attempts to execute it directly.</li>
     * </ul>
     * <p>
     * The process is launched in a detached manner, and its output is redirected to log files.
     * </p>
     * * @param fileName The name or path of the script/executable file to run.
     * @param serviceId A unique identifier for the service being started.
     * @param port The port number to be used by the service, primarily for Java JAR workers.
     * @return A status string indicating success or failure of the launch operation.
     */
    private String startProcess(String fileName, String serviceId, String port){
        // Payload: "filename|service_id"
//        File scriptFile = new File(WORKSPACE_DIR, fileName);

        File scriptFile = new File(fileName);
        // If it's NOT absolute (standard deploy), append workspace dir.
        // If it IS absolute (archive deploy), leave it alone.
        if (!scriptFile.isAbsolute()) {
            scriptFile = new File(WORKSPACE_DIR, fileName);
        }

        if(!scriptFile.exists()){
            return "ERROR: File not found at " + scriptFile.getAbsolutePath();
        }

        boolean isWorkerJar = fileName.equalsIgnoreCase("Worker.jar");

        if (isWorkerJar) {
            try {
                String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
                if (System.getProperty("os.name").toLowerCase().contains("win")) javaBin += ".exe";

                // We use the absolute path to the JAR we just staged in titan_workspace
                String jarPath = scriptFile.getAbsolutePath();

                ProcessBuilder pb;
                if (System.getProperty("os.name").toLowerCase().contains("win")) {
                    // Windows Detached: cmd /c start /b java -jar Worker.jar <port>
                    pb = new ProcessBuilder("cmd", "/c", "start", "/b", javaBin, "-jar", "\"" + jarPath + "\"", port);
                } else {
                    // Linux Detached: nohup java -jar Worker.jar <port> &
                    pb = new ProcessBuilder("nohup", javaBin, "-jar", jarPath, port, "&");
                }

                pb.redirectOutput(ProcessBuilder.Redirect.appendTo(new File(WORKSPACE_DIR + "/worker_" + port + ".log")));
                pb.redirectError(ProcessBuilder.Redirect.appendTo(new File(WORKSPACE_DIR + "/worker_" + port + "_error.log")));
                pb.redirectInput(ProcessBuilder.Redirect.from(new File(System.getProperty("os.name").contains("Win") ? "NUL" : "/dev/null")));

                pb.directory(new File(WORKSPACE_DIR));

                Process p = pb.start();

                ProcessRegistry.register(serviceId, p.pid());

//                p.onExit().thenRun(() -> {
//                    titan.tasks.ProcessRegistry.unregister(serviceId);
//                    System.out.println("[INFO] Worker JAR Stopped: " + serviceId);
//                });

                System.out.println("[DEBUG] Launched Detached JAR: " + jarPath + " on port " + port);
                return "DEPLOYED_SUCCESS | ID: " + serviceId + " | PID: DETACHED";
            } catch (IOException e) {
                return "LAUNCH_ERROR: " + e.getMessage();
            }
        } else{
            if (fileName.endsWith(".py")) {
                return launchDetachedProcess(serviceId, scriptFile.getParentFile(), "python", scriptFile.getAbsolutePath());
            } else {
                return launchDetachedProcess(serviceId, scriptFile.getParentFile(), scriptFile.getAbsolutePath());
            }
        }

    }

    /**
     * Launches a generic process in a detached mode, managing its lifecycle and log streaming.
     * <p>
     * This method performs the following steps:
     * </p>
     * <ol>
     * <li>Checks if a service with the given {@code serviceId} is already running.</li>
     * <li>Configures a {@link ProcessBuilder} with the provided command and redirects standard error/output to a local log file.</li>
     * <li>Sets the working directory for the process.</li>
     * <li>Starts the process.</li>
     * <li>Spawns a new thread to continuously read the process's output, write it to a local log file, and batch-stream it to the master server.</li>
     * <li>Registers the process in the {@code runningServices} map and with the {@link ProcessRegistry}.</li>
     * <li>Registers an {@code onExit} hook to clean up resources, remove the service from registries, and notify the master when the process terminates.</li>
     * </ol>
     * * @param serviceId A unique identifier for the service.
     * @param executionDir The directory in which the command should be executed. If null or non-existent, {@link #WORKSPACE_DIR} is used.
     * @param command The command and its arguments to execute.
     * @return A status string indicating success or failure, including the service ID and PID if successful.
     */
    private String launchDetachedProcess(String serviceId, File executionDir, String... command) {
        if (runningServices.containsKey(serviceId)) {
            return "SERVICE_ALREADY_RUNNING: " + serviceId;
        }

        try {
            ProcessBuilder pb = new ProcessBuilder(command);

            // 2. Separate Logs (Crucial for debugging background jobs)
//            File logFile = new File(WORKSPACE_DIR, serviceId + ".log");
//            pb.redirectOutput(logFile);
//            pb.redirectError(logFile);
            pb.redirectErrorStream(true);

            if (executionDir != null && executionDir.exists()) {
                pb.directory(executionDir);
            } else {
                pb.directory(new File(WORKSPACE_DIR));
            }

            // 3. START (Async/Detached)
            Process process = pb.start();

            new Thread(() -> {
                LogBatcher batcher = new LogBatcher(
                        serviceId,
                        parentServer.getSchedulerHost(),
                        parentServer.getSchedulerPort()
                );

                File logFile = new File(WORKSPACE_DIR, serviceId + ".log");
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                     FileWriter fw = new FileWriter(logFile, true)) { // 'true' for append mode

                    String line;
                    while ((line = reader.readLine()) != null) {
                        // Save to local disk History
                        fw.write(line + System.lineSeparator());
                        fw.flush();

                        batcher.addLog(line);

                        // Stream to Master (Real-time)
//                        parentServer.streamLogToMaster(serviceId, line);
                    }
                } catch (IOException e) {
                    System.out.println("[STREAM END] " + serviceId + " finished.");
                }finally {
                    batcher.close();
                }
            }).start();

            // 4. Register in Memory Map
            runningServices.put(serviceId, process);

            long pid = process.pid();
            titan.tasks.ProcessRegistry.register(serviceId, pid);

            // 5. Clean up Map when process dies
            process.onExit().thenRun(() -> {
                runningServices.remove(serviceId);
                titan.tasks.ProcessRegistry.unregister(serviceId);
                System.out.println("[INFO] Service Stopped: " + serviceId);
                parentServer.notifyMasterOfServiceStop(serviceId);
            });

            return "DEPLOYED_SUCCESS | ID: " + serviceId + " | PID: " + process.pid();

        } catch (IOException e) {
            e.printStackTrace();
            return "LAUNCH_ERROR: " + e.getMessage();
        }
    }

    /**
 * Attempts to stop a running service identified by its {@code serviceId}.
 * <p>
 * It retrieves the {@link Process} object associated with the {@code serviceId} from {@link #runningServices},
 * then attempts to destroy the process. If the process is found and terminated, it is removed from the map.
 *</p>
 * @param serviceId The unique identifier of the service to stop.
 * @return A status string indicating whether the service was stopped or if it was unknown.
 */
    private String stopProcess(String serviceId){
        Process p = runningServices.get(serviceId);
            if(p!=null){
                p.destroy();
                runningServices.remove(serviceId);
                return "STOPPED: " + serviceId;
            } else{
                return "UNKNOWN_SERVICE: " + serviceId;
            }
    }
}

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

public class ServiceHandler implements TaskHandler {
    private static final Map<String, Process> runningServices = new ConcurrentHashMap<>();
    private static final String WORKSPACE_DIR = "./titan_workspace/";
    private final String operation;

    private final RpcWorkerServer parentServer;

    public ServiceHandler(String op, RpcWorkerServer parentServer){
        this.operation = op;
        this.parentServer = parentServer;
    }

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

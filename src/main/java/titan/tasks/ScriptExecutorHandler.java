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

import titan.network.RpcWorkerServer;
import titan.tasks.TaskHandler;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import titan.network.LogBatcher;

/**
 * {@code ScriptExecutorHandler} is an implementation of {@link TaskHandler} responsible for executing scripts
 * (Python, Shell, or standalone executables) on the worker node.
 * It manages a dedicated workspace for script execution, handles payload parsing,
 * process execution, real-time log streaming, and result reporting back to the master.
 * <p>
 * This handler supports different execution contexts:
 * <ul>
 *     <li><b>Isolated:</b> Each job gets its own subdirectory within the root workspace.</li>
 *     <li><b>Shared:</b> A common 'shared' directory for DAG-related tasks.</li>
 *     <li><b>Archive Mode:</b> Executes scripts directly within their unzipped absolute path.</li>
 * </ul>
 * It also provides robust parsing for job payloads, supporting both new (JOB_ID | FILENAME | ARGS)
 * and legacy formats.
 */
    public class ScriptExecutorHandler implements TaskHandler {
    /**
     * The base directory name for all Titan worker workspaces.
     * All job-specific and shared workspaces will be created under this directory.
     */
    private static final String WORKSPACE_DIR = "titan_workspace";
    /**
     * A reference to the parent {@link RpcWorkerServer} instance.
     * Used for interacting with the master, specifically for streaming logs.
     */
    private final RpcWorkerServer parentServer;

    /**
     * The root {@link File} object representing the base directory for all script executions.
     * This is typically {@code titan_workspace}.
     */
    private final File rootWorkspace;
    /**
     * The {@link File} object representing the shared workspace directory.
     * This directory is used for tasks that require shared resources, such as DAG executions.
     * It is located at {@code titan_workspace/shared}.
     */
    private final File sharedWorkspace;
    private final ConcurrentMap<String, Process> activeProcesses = new ConcurrentHashMap<>();
    private final java.util.Set<String> cancelled = ConcurrentHashMap.newKeySet();

    /** Kill an active process, or remember a cancellation racing with process startup. */
    public void cancel(String jobId) {
        cancelled.add(jobId);
        Process process = activeProcesses.get(jobId);
        if (process != null) terminateProcessTree(process);
    }

    /**
     * Constructs a new {@code ScriptExecutorHandler}.
     * Initializes the root and shared workspace directories, creating them if they do not exist.
     *
     * @param parentServer The {@link RpcWorkerServer} instance that this handler is associated with.
     */
    public ScriptExecutorHandler(RpcWorkerServer parentServer) {
        this.parentServer = parentServer;

        this.rootWorkspace = new File(WORKSPACE_DIR);
        if (!rootWorkspace.exists()) {
            boolean created = rootWorkspace.mkdirs();
            if(created) System.out.println("[INIT] Created Root Workspace: " + rootWorkspace.getAbsolutePath());
        }

        this.sharedWorkspace = new File(rootWorkspace, "shared");
        if (!sharedWorkspace.exists()) {
            sharedWorkspace.mkdirs();
            System.out.println("[INIT] Created Shared DAG Workspace: " + sharedWorkspace.getAbsolutePath());
        }
    }

    /**
     * Executes a script based on the provided payload.
     * The payload is parsed to extract the job ID, script filename, and any arguments.
     * The script is then executed in an appropriate workspace directory (isolated, shared, or archive mode).
     * Standard output and error streams of the script are captured and streamed to the master via a {@link LogBatcher}.
     * DAG scripts may run for up to 24 hours; standalone scripts retain a 60-second limit.
     *
     * <p>Payload Format (New): {@code JOB_ID | FILENAME | ARGS}
     * <p>Payload Format (Legacy): {@code FILENAME | ... | JOB_ID} (where ARGS are not explicitly passed)
     *
     * @param payload The string payload containing script execution details.
     * @return A string indicating the execution status and result. Format: {@code COMPLETED|ExitCode|OutputContent}
     *         An execution error throws an exception for the worker to report as a failed callback.
     */
    @Override
    public String execute(String payload) {
        // ROBUST PARSING (Standardized Format: JOB_ID | FILENAME | ARGS)
        // We limit split to 3 so that arguments containing pipes are retained
        String[] parts = payload.split("\\|", 3);

        String jobId = "UNKNOWN";
        String filename = "";
        String args = "";

//        String filename = parts[0];
//        System.out.println("GIVEN PAYLOAD TO SCRIPT EXECUTOR " + payload);
//
//        String jobId = filename;
        if (parts.length >= 2 && RuntimeCommandResolver.isSupportedScript(parts[1])) {
            // It is likely NEW format: JOB-123 | script.py | args
            jobId = parts[0];
            filename = parts[1];
            if (parts.length > 2) args = parts[2];
        }
        // This way I am just maintaining the previous crap parsing just for backward compatability where args are not passed
        // OF: (Filename | ... | ID)
        // Args remain null if old format is used
        else {
            filename = parts[0];
            // Handle RUN_PAYLOAD prefix edge case
            if (filename.equals("RUN_PAYLOAD") && parts.length > 1) {
                filename = parts[1];
            }

            // Extract Job ID from the LAST part
            if (parts.length > 1) {
                String lastPart = parts[parts.length - 1];
                if (!lastPart.equals(filename) && !lastPart.equals("RUN_PAYLOAD")) {
                    jobId = lastPart;
                }
            }
        }

        System.out.println("[INFO] Parsed -> Job: " + jobId + " | File: " + filename + " | Args: " + args);
        System.out.println("[INFO] [ScriptExecutor] Running: " + filename + " (ID: " + jobId + ")");
//        File scriptFile = new File(WORKSPACE_DIR + File.separator + filename);
//        File scriptFile = new File(rootWorkspace, filename);
        File scriptFile = new File(filename);

        if (!scriptFile.isAbsolute()) {
            // If it's relative (Normal execution), prepend the workspace root
            scriptFile = new File(rootWorkspace, filename);
        }

        if (!scriptFile.exists()) throw new IllegalArgumentException("Script file not found: " + filename);

        LogBatcher batcher = new LogBatcher(
                jobId,
                parentServer.getSchedulerHost(),
                parentServer.getSchedulerPort()
        );

        try {

            File executionDir;
            // 3. Selection Logic (No redundant mkdirs for Shared)
            if (scriptFile.isAbsolute()) {
                // ARCHIVE MODE: Run inside the unzipped folder (like in titan_workspace/JOB-123/)
                executionDir = scriptFile.getParentFile();
            }
            else if (jobId.startsWith("DAG-")) {
                // Use the pre-calculated shared folder
                executionDir = this.sharedWorkspace;
            } else {
                // ISOLATED: This MUST be created on the run, as it is unique to this job
                executionDir = new File(rootWorkspace, jobId);
                if (!executionDir.exists()) executionDir.mkdirs();
            }

            List<String> command = new ArrayList<>(RuntimeCommandResolver.commandFor(scriptFile));

            if(!args.isEmpty()){
                String [] argList = args.split(" ");
                for(String arg: argList){
                    if(!arg.trim().isEmpty()){
                        command.add(arg.trim());
                    }
                }
            }

            ProcessBuilder pb = new ProcessBuilder(command);

            pb.directory(executionDir);
            pb.environment().put("TITAN_HOST", parentServer.getSchedulerHost());
            pb.environment().put("TITAN_PORT", String.valueOf(parentServer.getSchedulerPort()));
            // Combine Errors with Output
            pb.redirectErrorStream(true);

            System.out.println("[INFO] Context: " + executionDir.getName());
            System.out.println("[INFO] Executing Command: " + command);

            // 3. Start Process
            if (cancelled.contains(jobId)) throw new IllegalStateException("Job cancelled before start");
            Process process = pb.start();
            activeProcesses.put(jobId, process);
            if (cancelled.contains(jobId)) terminateProcessTree(process);

            final String finalJobId = jobId;
            StringBuilder finalOutput = new StringBuilder();
            java.util.concurrent.atomic.AtomicReference<IOException> streamError =
                    new java.util.concurrent.atomic.AtomicReference<>();
            Thread streamer = new Thread(() -> {
                File logFile = new File(executionDir, finalJobId + ".log");
                System.out.println("[DEBUG] Writing logs to: " + logFile.getAbsolutePath());
                try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                   BufferedWriter fileWriter = new BufferedWriter(new FileWriter(logFile, true))){
                       String line;
                       while ((line = bufferedReader.readLine()) != null) {
//                           parentServer.streamLogToMaster(finalJobId, line);
                           batcher.addLog(line);

                           fileWriter.write(line);
                           fileWriter.newLine();
                           fileWriter.flush();

//                           System.out.println("[STREAM] " + line);

                           synchronized (finalOutput) {
                               finalOutput.append(line).append("\n");
                           }
                       }
               }catch (IOException e) { streamError.set(e); }
            });

            streamer.start();

            // 4. Wait for completion
            long timeoutSeconds = timeoutSeconds(jobId);
            boolean finished;
            try {
                finished = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                terminateProcessTree(process);
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Script execution interrupted", e);
            }
            if (!finished) {
                terminateProcessTree(process);
                streamer.join(1000);
                throw new IllegalStateException("Script timed out (" + timeoutSeconds + "s limit)");
            }
            streamer.join(); // this ensures we capture the last line of the o/p.
            if (streamError.get() != null) {
                throw new IllegalStateException("Could not capture script output", streamError.get());
            }

            // 5. Read Output (Byte-oriented for TitanProtocol compatibility)
//            byte[] outputBytes = process.getInputStream().readAllBytes();
//            String output = new String(outputBytes, StandardCharsets.UTF_8).trim();
            if (cancelled.contains(jobId)) throw new IllegalStateException("Job cancelled");
            int exitCode = process.exitValue();
            // Format: COMPLETED|ExitCode|OutputContent
            return "COMPLETED|" + exitCode + "|" + finalOutput.toString().trim();

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Execution failed: " + e.getMessage(), e);
        } finally {
            activeProcesses.remove(jobId);
            cancelled.remove(jobId);
            batcher.close();
        }
    }

    public static long timeoutSeconds(String jobId) {
        if (jobId != null && jobId.startsWith("DAG-hitl-gate-")) return 49 * 60 * 60;
        return jobId != null && jobId.startsWith("DAG-") ? 24 * 60 * 60 : 60;
    }

    private static void terminateProcessTree(Process process) {
        process.descendants().forEach(child -> child.destroyForcibly());
        process.destroyForcibly();
    }
}

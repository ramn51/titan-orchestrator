/*
Copyright 2026 Ram Narayanan

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

 */



package titan.scheduler;

import titan.filesys.AssetManager;
import titan.network.TitanProtocol;
import titan.network.RpcClient;
import titan.network.SchedulerServer;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.DelayQueue;

public class Scheduler {
    private final WorkerRegistry workerRegistry;
    private final RpcClient schedulerClient;
//    private final Queue<Job> taskQueue;
    private final BlockingQueue<Job> taskQueue;
    private final BlockingQueue<ScheduledJob> waitingRoom;
    private final Queue<Job> deadLetterQueue;
    private final SchedulerServer schedulerServer;

    private final ScheduledExecutorService heartBeatExecutor;
    private final ExecutorService dispatchExecutor;
    private final ExecutorService serverExecutor;
    private volatile boolean isRunning = true;
    private int port;

    // This is purely for validation purpose
    private final Map<String, TaskExecution> executionHistory = new ConcurrentHashMap<>();
    private final Map<String, Worker> liveServiceMap = new ConcurrentHashMap<>();

    // For history maintenance of (tasks all types).
    private final Map<String, Integer> workerCompletionStats = new ConcurrentHashMap<>();
    private final Map<String, java.util.Deque<Job>> workerRecentHistory = new ConcurrentHashMap<>();

    // Map to hold Active Job Objects for Async Retries
    private final Map<String, Job> runningJobs = new ConcurrentHashMap<>();
    private final Map<String, Job> dagWaitingRoom;

    // AutoScaling declarations
    int MAX_WORKERS = 5;
    private volatile boolean scalingInProgress = false;
    private final ScheduledExecutorService scalerExecutor = Executors.newSingleThreadScheduledExecutor();
    // Remember bad ports for avoiding during scaling
    private final Set<Integer> portBlacklist = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // This is related to log streaming
    private final Map<String, List<String>> liveLogBuffer = new ConcurrentHashMap<>();
    private static final int MAX_LOG_LINES = 100;

    public Scheduler(int port){
        workerRegistry = new WorkerRegistry();
        schedulerClient = new RpcClient(workerRegistry);
//        this.taskQueue = new ConcurrentLinkedDeque<>();
        this.taskQueue = new PriorityBlockingQueue<>();
        this.deadLetterQueue = new ConcurrentLinkedDeque<>();
        this.waitingRoom = new DelayQueue<>();
        this.dagWaitingRoom = new ConcurrentHashMap<>();

        this.port = port;
        this.heartBeatExecutor = Executors.newSingleThreadScheduledExecutor();
        this.dispatchExecutor = Executors.newSingleThreadExecutor();
        this.serverExecutor = Executors.newSingleThreadExecutor();
        try{
            this.schedulerServer = new SchedulerServer(port, this);
        } catch (IOException e){
            throw new RuntimeException("Failed to start Scheduler Server", e);
        }

        Thread clockWatcher = new Thread(() -> {
            System.out.println("Clock Watcher Started...");
            while (isRunning) {
                try {
                    ScheduledJob readyJob = waitingRoom.take();
                    System.out.println("Time is up Moving Job " + readyJob.getJob().getId() + " to Active Queue.");
                    taskQueue.add(readyJob.getJob());

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        clockWatcher.setDaemon(true);
        clockWatcher.start();
    }

    public void start(){
        System.out.println("Scheduler Core starting at port " + this.port);

//        new Thread(() -> schedulerServer.start()).start();
        serverExecutor.submit(() -> {
            try {
                schedulerServer.start();
            } catch (Exception e) {
                System.err.println("[FAIL] Scheduler Server crashed: " + e.getMessage());
            }
        });

        heartBeatExecutor.scheduleAtFixedRate(
                this::checkHeartBeat,
                5, 10, TimeUnit.SECONDS
        );

        dispatchExecutor.submit(() -> {
            try {
                runDispatchLoop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore interrupt status
                System.out.println("[ERROR] Dispatch Loop stopped.");
            } catch (Throwable t) {
                // Catches RuntimeException, NoClassDefFoundError, OutOfMemoryError, etc.
                System.err.println("CRITICAL: Dispatch Loop Died Unexpectedly!");
                t.printStackTrace();
            }
        });
    }

    public WorkerRegistry getWorkerRegistry(){
        return workerRegistry;
    }

    // AutoScale methods and helpers

    public void startAutoScaler(){
        System.out.println("[INFO] Titan Auto-Scaler active.");
        scalerExecutor.scheduleAtFixedRate(this::reconcileClusters, 15, 15, TimeUnit.SECONDS);
    }

    private synchronized void reconcileClusters(){
        try {
            List<Worker> allWorkers = new ArrayList<>(workerRegistry.getWorkers());
            if (allWorkers.isEmpty()) {
                System.out.println("[SCALER] No workers found. Skipping...");
                return;
            }

            if (scalingInProgress) return;

            List<Worker> generalWorkers = allWorkers.stream()
                    .filter(w -> w.capabilities().contains("GENERAL"))
                    .toList();

            boolean generalPoolSaturated;
            if (generalWorkers.isEmpty()) {
                generalPoolSaturated = true;
            } else {
                generalPoolSaturated = generalWorkers.stream().allMatch(Worker::isSaturated);
            }

            long busyCount = allWorkers.stream().filter(w -> w.currentJobId != null).count();
            int totalCount = allWorkers.size();
            int totalUsedSlots = allWorkers.stream().mapToInt(Worker::getCurrentLoad).sum();
            int totalAvailableSlots = allWorkers.stream().mapToInt(Worker::getMaxCap).sum();;
            System.out.println("[SCALER] Cluster Pressure: " + totalUsedSlots + "/" + totalAvailableSlots);

            if (generalPoolSaturated && totalCount < MAX_WORKERS){
               scalingInProgress = true;
//               int nextPort = workers.stream().mapToInt(Worker::port).max().orElse(8080) + 1;
                // If the next port is not available then we just dont do scale up itself.
//               int nextPort = findSafePort(workers);
                int nextPort = findSafePort(allWorkers, 8090, 8200);

               if (nextPort == -1) {
                    System.err.println("[SCALER] Could not find any open ports. Aborting scale-up.");
                    scalingInProgress = false;
                    return;
               }

               System.out.println("[SCALER] Cluster saturated (" + busyCount + "/" + totalCount + "). Scaling to port: " + nextPort);
               String serviceId = "WRK-" + nextPort + "-" + UUID.randomUUID().toString().substring(0, 8);
               String autoScalePayload = "DEPLOY_PAYLOAD|Worker.jar|INTERNAL_SCALE|" + nextPort;

                // Mark this as High priority
               Job scaleJob = new Job(autoScalePayload, 10, 0);
               scaleJob.setId(serviceId);
               // Instead of taskQueue.add(scaleJob) preferably in future its better to go for forceInception rather than queued way.
               this.submitJob(scaleJob);
               return;
            }

            // For scale down
            // Only scale down if the WHOLE cluster is idle and we have more than 1 worker
            Set<Worker> serviceHosts = new HashSet<>(liveServiceMap.values());

            if (!generalPoolSaturated && totalCount > 1) {
                Worker idleTarget = allWorkers.stream()
                        .filter(w -> !w.isPermanent())
                        .filter(w -> w.port() != 8080) // Never kill the root
                        .filter(w -> w.getCurrentLoad() == 0) // Must be doing nothing
                        .filter(w -> !serviceHosts.contains(w)) // Don't kill if hosting a Service
                        .filter(w -> w.getIdleDuration() > 45000) // Idle for > 45 seconds
                        .max(java.util.Comparator.comparingInt(Worker::port)) // Kill highest port first
                        .orElse(null);

                if (idleTarget != null) {
                    System.out.println("[SCALER] SCALE-DOWN: Worker " + idleTarget.port() + " is excess capacity. Removing.");
                    this.shutdownWorkerNode(idleTarget.host(), idleTarget.port());
                    // 2. Remove from local registry immediately
                    workerRegistry.getWorkerMap().remove(idleTarget.host() + ":" + idleTarget.port());
                }
            }
        } catch (Exception e) {
            System.err.println("[SCALER ERROR] " + e.getMessage());
            this.scalingInProgress = false;
        }
    }

    // Helper methods related to scaling. Finding the available ports for new spawning.
    private int findSafePort(List<Worker> currentWorkers, int minRange, int maxRange) {
        int maxCurrentPort = currentWorkers.stream()
                .mapToInt(Worker::port)
                .filter(p -> p >= minRange && p <= maxRange)
                .max()
                .orElse(minRange - 1);

        int startPort = maxCurrentPort + 1;
        // Scan up to 20 ports to find a free one
        for (int p = startPort; p < startPort + 20; p++) {
            if (!isPortInUseLocally(p) && !portBlacklist.contains(p)) {
                return p;
            }
            System.out.println("[SCALER] Port " + p + " is busy on OS. Skipping...");
        }
        return -1;
    }

    private boolean isPortInUseLocally(int port) {
        try (Socket ignored = new Socket("localhost", port)) {
            return true;
        } catch (IOException e) {
            return false; // Connection refused = Port is free
        }
    }

    public void checkHeartBeat(){
        System.out.println("Sending Heartbeat");
        for(Worker worker: workerRegistry.getWorkers()){

            String result = schedulerClient.sendRequest(
                    worker.host(),
                    worker.port(),
                    TitanProtocol.OP_HEARTBEAT,
                    ""
            );

            if(result  == null){
                workerRegistry.markWorkerDead(worker.host(), worker.port());
            } else if(result.startsWith("PONG")){
                worker.updateLastSeen();
                String[] parts = result.split("\\|");
                if (parts.length > 1) {
                    int load = Integer.parseInt(parts[1]);
                    worker.setCurrentLoad(load);
                    if(load > 0){
                        System.out.println("Worker " + worker.port() + "Has load" + worker.getCurrentLoad());
                    }
                }
                if (parts.length > 2) {
                    int maxCapacity = Integer.parseInt(parts[2]);
                    worker.setMaxCap(maxCapacity);
                }
                workerRegistry.updateLastSeen(worker.host(), worker.port());
            }
        }
    }

    public Map<String, Worker> getLiveServiceMap(){
        return liveServiceMap;
    }

    public synchronized void registerWorker(String host, int port, String capability, boolean isPermanent) {
        this.workerRegistry.addWorker(host, port, capability, isPermanent);
        this.scalingInProgress = false;
        this.portBlacklist.remove(port);

//        System.out.println("[DEBUG] Attempting promotion for incoming worker at " + host + ":" + port);
        System.out.println("[INFO] New Worker Registered: " + host + ":" + port +
                (isPermanent ? " [PERMANENT]" : " [EPHEMERAL]"));

        liveServiceMap.entrySet().removeIf(entry -> {
                    String serviceId = entry.getKey();
                    boolean idMatches = serviceId.startsWith("WRK-" + port + "-");

                    if (idMatches) {
                        System.out.println("[PROMOTION] Job " + serviceId + " converted to Peer.");

                        // 2. Find the Parent who was working on this and set them to Idle
                        for (Worker w : workerRegistry.getWorkers()) {
                            if (serviceId.equals(w.currentJobId)) {
                                w.currentJobId = null; // Parent is now free!
                                System.out.println("[DEBUG] Parent Worker " + w.port() + " is now IDLE.");
                            }
                        }
                        return true;
                    }

                return false; // Removes "WRK-..." from the parent's Running Services list
            });
    }

    public Map<String, Job> getDAGWaitingRoom(){
        return dagWaitingRoom;
    }

    public void submitJob(Job job){
        System.out.println("[INFO] [DAG] Job " + job.getId() + " is waiting.");
        if (!job.isReady()) {
            System.out.println("[INFO] Job " + job.getId() + " blocked by dependencies. Entering DAG Waiting Room.");
            dagWaitingRoom.put(job.getId(), job);
            return;
        }

        long delay = job.getScheduledTime() - System.currentTimeMillis();
        if(delay <=0){
            // Run the job now (Add to the queue, dispatcher will do the polling and execution)
            System.out.println(" ** Queueing Job: " + job.getId());
            taskQueue.add(job);
        } else{
            System.out.println("[INFO] Job Delayed: " + job.getId() + " for " + delay + "ms");
            waitingRoom.add(new ScheduledJob(job));
        }
    }

    public void submitJob(String jobPayload) {
        System.out.println("** Scheduler received job: " + jobPayload);

        String temp = jobPayload.trim();
        long delay = 0;
        int priority = 1;

        // Parse DELAY from the Right ---
        // We look for the last pipe. If the text after it is a number, we take it and remove it.
        int lastPipe = temp.lastIndexOf('|');
        if (lastPipe != -1) {
            String suffix = temp.substring(lastPipe + 1);
            try {
                delay = Long.parseLong(suffix);
                temp = temp.substring(0, lastPipe); // Chop off the delay
            } catch (NumberFormatException e) {
                // It wasn't a number (e.g. it was part of base64 data). We leave the string alone.
            }
        }

        // Extract PRIORITY from the Right
        // Repeat the process for the next item on the right.
        lastPipe = temp.lastIndexOf('|');
        if (lastPipe != -1) {
            String suffix = temp.substring(lastPipe + 1);
            try {
                priority = Integer.parseInt(suffix);
                temp = temp.substring(0, lastPipe);
            } catch (NumberFormatException e) {
                // ignore, no action
            }
        }

        // 'temp' contains the ID and the Payload (Data, Req, etc.)
        // Example: "JOB-101 | RUN_PAYLOAD | script.py | data | GPU"
        // Identify and Separate ID (From the Left) ---
        int firstPipe = temp.indexOf('|');
        String potentialId = null;
        String actualPayload = temp;

        if (firstPipe != -1) {
            String prefix = temp.substring(0, firstPipe);

            // If it DOES NOT start with a command keyword, it must be a custom Job ID.
            if (!prefix.startsWith("RUN_PAYLOAD") &&
                    !prefix.startsWith("DEPLOY_PAYLOAD") &&
                    !prefix.startsWith("START_ARCHIVE") &&
                    !prefix.startsWith("RUN_ARCHIVE")) {

                potentialId = prefix; // "JOB-101"
                actualPayload = temp.substring(firstPipe + 1).trim(); // "RUN_PAYLOAD | script.py | data | GPU"
            }
        }

        Job job = new Job(actualPayload, priority, delay);
        if (potentialId != null) {
            job.setId(potentialId);
        }

        submitJob(job);
    }

    private String extractSkillRequirement(Job job) {
        String payload = job.getPayload();
        if (payload == null || payload.isEmpty()) return "GENERAL";

        if (payload.contains("INTERNAL_SCALE")) {
            return "GENERAL";
        }

        String[] parts = payload.split("\\|");
        // 1. Trim everything
        for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();

        // 2. FIND ANCHOR (Command Header)
        int headerIndex = -1;
        for (int i = 0; i < Math.min(parts.length, 3); i++) {
            String p = parts[i];
            if (p.equals("RUN_PAYLOAD") || p.equals("DEPLOY_PAYLOAD") ||
                    p.equals("RUN_ARCHIVE") || p.equals("START_ARCHIVE_SERVICE")) {
                headerIndex = i;
                break;
            }
        }

        if (headerIndex == -1) {
            // Safety: If payload starts with ID, don't return ID as skill
            if (parts.length > 0 && parts[0].equals(job.getId())) return "GENERAL";
            return (parts.length > 0) ? parts[0] : "GENERAL";
        }

        // 3. SCAN BACKWARDS (Peel off Metadata)
        int endIndex = parts.length - 1;

        while (endIndex > headerIndex) {
            String p = parts[endIndex];
            boolean isMetadata = false;

            // --- THE FIX: IGNORE JOB ID AT THE END ---
            if (p.equals(job.getId())) isMetadata = true;

                // Check standard metadata
            else if (p.startsWith("[") && p.endsWith("]")) isMetadata = true; // Parents
            else if (p.equals("AFFINITY")) isMetadata = true;            // Affinity Tag
            else {
                try {
                    Long.parseLong(p); // Priority or Delay
                    isMetadata = true;
                } catch (NumberFormatException ignored) {}
            }

            if (isMetadata) {
                endIndex--; // Skip this token
            } else {
                // We found a non-metadata string.
                if (endIndex != headerIndex) {
                    // Sanity Check
                    if (p.length() < 20 && !p.isEmpty() && !p.endsWith("=")) {
                        return p;
                    }
                }
                break;
            }
        }

        return "GENERAL";
    }

    private void runDispatchLoop() throws InterruptedException {
        System.out.println("Running Dispatch Loop");
        while (isRunning) {
                Job job = taskQueue.take();
                System.out.println("DEBUG: Processing Job ID: " + job.getId());
                job.setStatus(Job.Status.RUNNING);
//                history.put(job.getId(), job.getStatus());
                System.out.println(" Job Processing: " + job);

                String reqTaskSkill = extractSkillRequirement(job);

                System.out.println("[DISPATCH] Job " + job.getId() + " requires: [" + reqTaskSkill + "]");

                List<Worker> availableWorkers = workerRegistry.getWorkersByCapability(reqTaskSkill);

                if (availableWorkers.isEmpty()) {
                    if (!reqTaskSkill.equals("GENERAL")) {
                        System.out.println("[WAIT] No active workers found with capability: " + reqTaskSkill);
                    } else {
                        System.out.println("[WAIT] No GENERAL workers available.");
                    }

                    // Re-queue the job to try again later (Backpressure)
                    job.setStatus(Job.Status.PENDING);
                    taskQueue.add(job);
                    Thread.sleep(2000);
                    continue;
                }

                Worker selectedWorker = selectBestWorker(job, availableWorkers);

                if (selectedWorker == null) {
                    System.out.println("All workers SATURATED or unavailable. Re-queueing job.");
                    job.setStatus(Job.Status.PENDING);
                    taskQueue.put(job); // Use put for blocking
                    Thread.sleep(1000);
                    continue;
                }
                selectedWorker.incrementCurrentLoad();
                TaskExecution record = new TaskExecution(job.getId(), selectedWorker);
                executionHistory.put(job.getId(), record);
                runningJobs.put(job.getId(), job);

//                Worker selectedWorker = availableWorkers.get(ThreadLocalRandom.current().nextInt(availableWorkers.size()));
                System.out.println("[INFO] Dispatching " + job.getId() + " to Worker " + selectedWorker.port());
                try{
                        String response = executeJobRequest(job, selectedWorker);
                        System.out.println("[OK] Job Finished: " + response);

                        if("JOB_ACCEPTED".equals(response)){
                            System.out.println("[ASYNC] Job " + job.getId() + " accepted by worker. Waiting for callback.");
                        }else {
                            System.out.println("[SYNC] Task finished immediately: " + response);
                            completeJob(job, response, record);

                            // NOTE: For Sync jobs (Deploy), we complete and decrement immediately here
                            // because they don't trigger the handleJobCallback.
                            selectedWorker.decrementCurrentLoad();
                        }

                } catch (Exception e){
                    System.err.println("[FAIL] Job " + job.getId() + " Error: " + e.getMessage());
                    record.fail(e.getMessage());

                    runningJobs.remove(job.getId());

                    if (selectedWorker != null) {
                        selectedWorker.currentJobId = null;
                        selectedWorker.decrementCurrentLoad();
                        String wKey = String.valueOf(selectedWorker.port());
                        workerRecentHistory.computeIfAbsent(wKey, k -> new java.util.concurrent.ConcurrentLinkedDeque<>()).add(job);
                        // Keep list size in check
                        if (workerRecentHistory.get(wKey).size() > 10) {
                            workerRecentHistory.get(wKey).removeFirst();
                        }
                    }

                    if (e.getMessage().contains("SATURATED")) {
                        System.out.println("[WARN] Worker " + selectedWorker.port() + " is Saturated (Optimistic check failed). Penalizing.");
                        selectedWorker.setCurrentLoad(99);
                    }


                    // We fail the job here and not give it retry if its a deployment issue
                    if (e.getMessage().contains("ALREADY in use") || e.getMessage().contains("Deployment Rejected")) {
                        System.err.println("[FAIL-FAST] Non-recoverable error. Cancelling retries.");
                        job.setStatus(Job.Status.FAILED);
                        // We do NOT call handleJobFailure(job) here, so it won't retry/become DEAD.
                    } else{
                        handleJobFailure(job);
//                    history.put(job.getId(), job.getStatus());
                    }
                }
            }
        }

    private void handleJobFailure(Job job) {
        // If the worker is autoscaled messed up then immediately fail it and not send it to the queue for retry
        if (job.getId().startsWith("WRK-")) {
            System.err.println("[SCALER] Auto-scale job " + job.getId() + " failed. Abandoning job so Scaler can pick a new port.");
            job.setStatus(Job.Status.FAILED);
            return;
        }

        TaskExecution record = executionHistory.get(job.getId());
        job.incrementRetry();
        if(job.getRetryCount() > 3) {
            job.setStatus(Job.Status.DEAD);

            if (record != null) record.status = Job.Status.DEAD;
//            history.put(job.getId(), Job.Status.DEAD);
            System.err.println("Job Moved to DLQ (Max Retries): " + job);
            this.deadLetterQueue.offer(job);
            cancelChildren(job.getId());

        } else{
            job.setStatus(Job.Status.FAILED);
            System.err.println("Job Failed. Retrying... (" + job.getRetryCount() + "/3)");
            job.setStatus(Job.Status.PENDING);
            // We leave the record as FAILED for now so history shows it failed
            //history.put(job.getId(), Job.Status.PENDING);
            taskQueue.offer(job);
        }
    }

    public void handleJobCallback(String payload){
        // Payload: "JOB-123|COMPLETED|Result: 5050"
        String[] parts = payload.split("\\|", 3);
        if (parts.length < 2) return;

        String jobId = parts[0];
        String statusStr = parts[1];
        String result = (parts.length > 2) ? parts[2] : "";

        TaskExecution record = executionHistory.get(jobId);
        Job job = runningJobs.remove(jobId);

        if(record != null){
            // Clear "Active Job" flag on Worker (Stop Pulse (for dash))
            if (record.assignedWorker != null) {
                record.assignedWorker.currentJobId = null;
            }

            if(statusStr.equals("COMPLETED")){
//                if(record.assignedWorker != null){
                    System.out.println("[ASYNC] Callback: Job " + jobId + " Finished.");
                    completeJob(job, result, record);

                    if (record.assignedWorker != null) {
                        record.assignedWorker.decrementCurrentLoad();
                    }
//                    if(job != null) job.setStatus(Job.Status.COMPLETED);
//                }
            } else {
                System.err.println("[ASYNC] [FAILED] Callback: Job " + jobId + " Failed.");
                record.fail(result);

                if (job != null) {
                    handleJobFailure(job);
                } else {
                    System.err.println("CRITICAL: Job object lost for " + jobId + ", cannot retry.");
                }
            }
        } else {
            System.err.println("[WARN] Received callback for unknown Job ID: " + jobId);
        }


    }

    private void completeJob(Job job, String result, TaskExecution record){
        record.complete(result);

        if (job != null) job.setStatus(Job.Status.COMPLETED);

        if (record.assignedWorker != null) {
            String wKey = String.valueOf(record.assignedWorker.port());
            workerCompletionStats.merge(wKey, 1, Integer::sum);

            if (job != null) {
                workerRecentHistory.computeIfAbsent(wKey, k -> new java.util.concurrent.ConcurrentLinkedDeque<>()).add(job);
                if (workerRecentHistory.get(wKey).size() > 10) {
                    workerRecentHistory.get(wKey).removeFirst();
                }
            }

            propagateAffinity(job.getId(), wKey);
        }
        unlockChildren(job.getId());
    }

    private Worker selectBestWorker(Job job, List<Worker> availableWorkers){
        if(job.getPreferredWorkerId() != null){
            for(Worker w: availableWorkers){
                if(String.valueOf(w.port()).equals(job.getPreferredWorkerId()) && !w.isSaturated()){
                    System.out.println("[AFFINITY] Sticky Scheduling: Routing Job " + job.getId() + " to Worker " + w.port());
                    return w;
                }
            }
        }

        // Get the least loaded worker
        Worker bestWorker = null;
        int minLoad = Integer.MAX_VALUE;
        for(Worker worker: availableWorkers){
            if(worker.isSaturated())
                continue;

            if(worker.getCurrentLoad() < minLoad){
                minLoad = worker.getCurrentLoad();
                bestWorker = worker;
            }
        }

        return bestWorker;
    }

    private void propagateAffinity(String parentId, String workerPortId) {
        boolean workerAlive = workerRegistry.getWorkers().stream()
                .anyMatch(w -> String.valueOf(w.port()).equals(workerPortId));

        if(!workerAlive) return;

        for (Job waitingJob : dagWaitingRoom.values()) {
            if (waitingJob.getDependenciesIds() != null && waitingJob.getDependenciesIds().contains(parentId)) {
                if(waitingJob.isAffinityRequired()){
                    if (waitingJob.getPreferredWorkerId() == null) {
                        waitingJob.setPreferredWorkerId(workerPortId);
                        System.out.println("[AFFINITY] Child " + waitingJob.getId() + " locked to Parent's Node: " + workerPortId);
                    }
                }
            }
        }
    }

    private String executeJobRequest(Job job, Worker worker) throws Exception {
        String rawPayload = job.getPayload();
        String actualPayload = rawPayload;
        worker.currentJobId = job.getId();

        boolean isSystemCommand = rawPayload.startsWith("DEPLOY_PAYLOAD") ||
                rawPayload.startsWith("RUN_PAYLOAD");

        if (rawPayload.contains("|")) {
            String[] parts = rawPayload.split("\\|", 2);
            String potentialCommand = parts[1];
            // Add detection for ARCHIVE commands
            if (potentialCommand.startsWith("RUN_ARCHIVE") ||
                    potentialCommand.startsWith("START_ARCHIVE_SERVICE")) {
                actualPayload = potentialCommand;
            } else if (potentialCommand.startsWith("DEPLOY_PAYLOAD") ||
                    potentialCommand.startsWith("RUN_PAYLOAD")) {
                actualPayload = potentialCommand;
            }
        }

//        System.out.println("[DEBUG] Dispatching Clean Payload: " + actualPayload);

        if (actualPayload.startsWith("DEPLOY_PAYLOAD")) {
            return executeDeploySequence(job, worker, actualPayload);
        } else if (actualPayload.startsWith("RUN_PAYLOAD")) {
            return executeRunOneOff(job, worker, actualPayload);
        } else if (actualPayload.startsWith("RUN_ARCHIVE")) {
            return executeRunArchive(job, worker, actualPayload);
        }
        else if (actualPayload.startsWith("START_ARCHIVE_SERVICE")) {
            return executeServiceArchive(job, worker, actualPayload);
        }
        else {
            return executeStandardTask(job, worker, actualPayload);
        }
    }

    private String executeStandardTask(Job job, Worker worker, String payload) throws Exception {
        // NEW FORMAT: "JOB-123|calc.py"
        String payloadWithId = job.getId() + "|" + payload;
        return sendExecuteCommand(worker, TitanProtocol.OP_RUN, payloadWithId);
    }

    private String executeDeploySequence(Job job, Worker worker, String payload) throws Exception {
        try {
//            String[] parts = payload.split("\\|", 4);
            String[] parts = payload.split("\\|");

            // Format: DEPLOY_PAYLOAD | filename | base64 | port | [DAG-ID]
            // DAG-ID is only if its a DAG job
            String filename = parts[1];
            String base64Script = parts[2];

            System.out.println("SCHEDULER LOGS::ARGS PASSED TO DEPLOY EXEC " + parts.length);

            if("INTERNAL_SCALE".equals(base64Script)){
                File localJar = new File("perm_files/Worker.jar");
                if (!localJar.exists()) {
                    throw new RuntimeException("Scaler Error: perm_files/Worker.jar not found on Master.");
                }
                byte[] fileContent = java.nio.file.Files.readAllBytes(localJar.toPath());
                base64Script = Base64.getEncoder().encodeToString(fileContent);
            }

            String portString = (parts.length > 3) ? parts[3] : null;
            int targetPort = -1;
            if (portString != null && !portString.isEmpty()) {
                // If User explicitly provided a port, need to verify this
                try {
                    targetPort = Integer.parseInt(portString);
                } catch (NumberFormatException e) {
                    // It wasn't a number (it was likely the DAG-ID).
                    // This means no port was provided. Default to 8085.
                    targetPort = 8085;
                    portString = "8085";
                }
            } else if (filename.contains("Worker.jar")) {
                // It's a Worker, but no port provided -> Default to 8085
                targetPort = 8085;
                portString = "8085";
            }

            if (targetPort != -1) {
                System.out.println("[DEPLOY] Checking if port " + targetPort + " is free...");
                if (isWorkerAlive(worker.host(), targetPort)) {
                    throw new RuntimeException("Deployment Rejected: Port " + targetPort + " is ALREADY in use by another service.");
                }
            }

            // Step 1: Stage
            String stagePayload = filename + "|" + base64Script;
            String stageResp = sendExecuteCommand(worker, TitanProtocol.OP_STAGE, stagePayload);
            if (!stageResp.contains("FILE_SAVED")) {
                throw new RuntimeException("Staging failed. Expected FILE_SAVED, got: " + stageResp);
            }
            System.out.println("[OK] File Staged");

            // Step 2: Start
            String safePortArg = (portString != null) ? portString : "0";
            String startPayload = filename + "|" + job.getId() + "|" + safePortArg;

            String startResp = sendExecuteCommand(worker, TitanProtocol.OP_START_SERVICE, startPayload);
            if (!startResp.contains("DEPLOYED_SUCCESS")) {
                throw new RuntimeException("Start failed. Expected DEPLOYED_SUCCESS, got: " + startResp);
            }

            String pid = startResp.contains("PID:") ? startResp.split("PID:")[1].trim() : "UNKNOWN";

            if (targetPort != -1) {
                boolean alive = false;

                // Try 10 times, once every 2 seconds
                for (int i = 1; i <= 10; i++) {
                    Thread.sleep(2000);
                    if (isWorkerAlive(worker.host(), targetPort)) {
                        alive = true;
                        System.out.println("[OK] Worker port " + targetPort + " detected on attempt " + i);
                        break;
                    }
                    System.out.println("[DEPLOY] Port " + targetPort + " not ready... (Attempt " + i + "/10)");
                }

                if (!alive) {
                    // LOCK RESET:  before throwing exception
                    if (job.getId().startsWith("WRK-")) {
                        this.scalingInProgress = false;
                    }
                    throw new RuntimeException("Deployment Failed: Process started (PID " + pid + ") but port " + targetPort
                            + " never became reachable after 20s.");
                }
            }

            liveServiceMap.put(job.getId(), worker);
            // Since deploy tasks are synchronous (kind of) so we clear it off and say its completed.
            worker.currentJobId = null;
            return "DEPLOYED_SUCCESS PID:" + pid;
        } catch (Exception e) {
            if (job.getId().startsWith("WRK-")) {
                // Unlock the scaler so it can try a different port in the next cycle
                this.scalingInProgress = false;
                try {
                    int failedPort = Integer.parseInt(job.getId().split("-")[1]);
                    portBlacklist.add(failedPort);
                    System.err.println("[SCALER] Blacklisting failed port: " + failedPort);
                } catch (Exception ignore) {}
            }
            throw e;
        }
    }

    private boolean isWorkerAlive(String host, int port) {
        try (Socket s = new Socket(host, port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }


    private String executeRunOneOff(Job job, Worker worker, String payload) throws Exception {
        String[] parts = payload.split("\\|");

        if(parts.length < 3) throw new RuntimeException("Invalid Run Payload");

        String filename = parts[1];
        String args = "";
        String base64Script = "";

        if (parts[2].length() > 50) {
            base64Script = parts[2];
            args = "";
        }
        // If parts[2] is small, it's likely ARGS, so the Base64 must be in parts[3]
        else if (parts.length >= 4) {
            args = parts[2];
            base64Script = parts[3];
        }
        else {
            // Fallback for short scripts without args
            base64Script = parts[2];
        }

        // STEP 1: STAGE (Same as Deploy)
//        String stageResp = sendExecuteCommand(worker, TitanProtocol.OP_DEPLOY, "STAGE_FILE|" + filename + "|" + base64Script);
        String stageResp = sendExecuteCommand(worker, TitanProtocol.OP_STAGE, filename + "|" + base64Script);
        if (!stageResp.contains("FILE_SAVED")) {
            throw new RuntimeException("Staging failed: " + stageResp);
        }
        System.out.println("[OK] File Staged for Run");

        // Use Async Run Protocol (this will run the script as a background and send status to main.java.titan.scheduler from worker)
        String payloadWithId = job.getId() + "|" + filename + "|" + args;
//        String runPayload = "RUN_PAYLOAD|" + filename + "|" + job.getId();
        return sendExecuteCommand(worker, TitanProtocol.OP_RUN, payloadWithId);
    }

    private String executeRunArchive(Job job, Worker worker, String payload) throws Exception {
        String [] parts = payload.split("\\|");

        String pointer = parts[1];
//        String args = (parts.length > 2) ? parts[2] : "";

        AssetManager.ArchiveInfo fileInfo = AssetManager.resolvePointer(pointer);

        String workerPayload = job.getId() + "|" + fileInfo.entryPoint + "|" + fileInfo.base64Content;

        System.out.println("[ARCHIVE] Dispatching Archive Job " + job.getId() + " (Zip: " + fileInfo.zipName + ")");

        return sendExecuteCommand(worker, TitanProtocol.OP_RUN_ARCHIVE, workerPayload);

    }

    private String executeServiceArchive(Job job, Worker worker, String payload) throws Exception {
        // Payload Format: START_ARCHIVE_SERVICE | zip_name.zip/entry.py | args | port
        String[] parts = payload.split("\\|");

        String pointer = parts[1];
        String args = (parts.length > 2) ? parts[2] : "";
        String port = (parts.length > 3) ? parts[3] : "8085";

        AssetManager.ArchiveInfo fileInfo = AssetManager.resolvePointer(pointer);

        // Worker Protocol for Service Archive: SERVICE_ID | ENTRY_FILE | PORT | BASE64_ZIP
        String workerPayload = job.getId() + "|" + fileInfo.entryPoint + "|" + port + "|" + fileInfo.base64Content;

        System.out.println("🚀 [ARCHIVE] Starting Service " + job.getId() + " on Port " + port);

        String response = sendExecuteCommand(worker, TitanProtocol.OP_START_SERVICE_ARCHIVE, workerPayload);

        if (response.contains("DEPLOYED_SUCCESS")) {
            liveServiceMap.put(job.getId(), worker);
            worker.currentJobId = null; // Since the Services are detached
        }
        return response;
    }

    private String sendExecuteCommand(Worker worker, byte opCode, String payload) throws Exception {
        String response = schedulerClient.sendRequest(worker.host(), worker.port(), opCode, payload);
        if (response == null || response.startsWith("ERROR") || response.startsWith("JOB_FAILED")) {
            System.err.println("[FAIL] Job Failed on Worker " + worker.port() + ": " + response);
            throw new RuntimeException("Worker Error: " + response);
        }
        return response;
    }

    public String stopRemoteService(String serviceId) {
        if (!liveServiceMap.containsKey(serviceId)) {
            return "ERROR: Service " + serviceId + " not found in liveServiceMap. Current keys: " + liveServiceMap.keySet();
        }
        Worker targetWorker = liveServiceMap.get(serviceId);
        if (targetWorker == null) {
            return "ERROR: Service " + serviceId + " not found.";
        }

        try {
            String response = sendExecuteCommand(targetWorker, TitanProtocol.OP_STOP, serviceId);
            if (response.contains("SUCCESS") || response.contains("STOPPED")) {
                liveServiceMap.remove(serviceId);
            }
            return response;
        } catch (Exception e) {
            return "COMMUNICATION_ERROR: " + e.getMessage();
        }
    }

    public String shutdownWorkerNode(String targetHost, int targetPort){
        Worker targetWorker = null;
        for(Worker w: this.getWorkerRegistry().getWorkers()){
            if (w.port() == targetPort && w.host().equals(targetHost)) {
                boolean exactMatch = w.host().equals(targetHost);
                boolean localAlias = (targetHost.equals("localhost") && w.host().equals("127.0.0.1")) ||
                        (targetHost.equals("127.0.0.1") && w.host().equals("localhost"));

                if (exactMatch || localAlias) {
                    targetWorker = w;
                    break;
                }
            }
        }

        if(targetWorker == null){
            return "ERROR: Worker node " + targetPort + " not found in registry.";
        }

        if (targetWorker.isPermanent()) {
            return "ERROR: Cannot auto-shutdown PERMANENT worker " + targetHost + ":" + targetPort;
        }

        System.out.println("[INFO] Initiating Graceful Shutdown for Worker " + targetPort);
        List<String> servicesToStop = new java.util.ArrayList<>();

        for(Map.Entry<String, Worker> entry: liveServiceMap.entrySet()){
            if(entry.getValue().equals(targetWorker)){
                servicesToStop.add(entry.getKey());
            }
        }

        for (String serviceId : servicesToStop) {
            System.out.println("[INFO] Stopping child service: " + serviceId);
            stopRemoteService(serviceId);
        }

        liveServiceMap.entrySet().removeIf(entry -> entry.getKey().contains("WRK-" + targetPort + "-"));
        // Send the Kill Command to the Worker
        try {
            schedulerClient.sendRequest(targetWorker.host(), targetWorker.port(), TitanProtocol.OP_KILL_WORKER, "NOW");
        } catch (Exception e) {
            System.err.println("[WARN] Worker might have died before receiving ACK: " + e.getMessage());
        }
        workerRegistry.getWorkerMap().remove(targetWorker.host() + ":" + targetWorker.port());
        return "SUCCESS: Worker " + targetPort + " and " + servicesToStop.size() + " services shut down.";
    }

    private void unlockChildren(String parentId){
        for(Job waitingJob: dagWaitingRoom.values()){
            if(waitingJob.getDependenciesIds()!=null && waitingJob.getDependenciesIds().contains(parentId)){
                waitingJob.resolveDependencies(parentId);

                if(waitingJob.isReady()){
                    System.out.println("[INFO] DAG: All dependencies met for " + waitingJob.getId() + ". Moving to Active Queue.");
                    dagWaitingRoom.remove(waitingJob.getId());
                    submitJob(waitingJob);
                }
            }
        }
    }

    public void cancelChildren(String failedParentId){
        for(Job job: dagWaitingRoom.values()){
            if(job.getDependenciesIds().contains(failedParentId)){
                System.err.println("[ERROR] Cancelling Job " + job.getId() + " because parent " + failedParentId + " failed.");

                job.setStatus(Job.Status.DEAD);
                // Create a Ghost execution record so getJobStatus() returns DEAD
                // We pass null for the worker because it never left the main.java.titan.scheduler.
                TaskExecution record = new TaskExecution(job.getId(), null);
                record.status = Job.Status.DEAD;
                record.endTime = System.currentTimeMillis(); // Died immediately
                record.output = "Cancelled: Parent " + failedParentId + " failed";

                executionHistory.put(job.getId(), record);

                dagWaitingRoom.remove(job.getId());
                this.deadLetterQueue.offer(job);
                cancelChildren(job.getId());
            }
        }
    }

    // Methods for sending the logs to stream to the UI
    public void logStream(String jobId, String line) {
        liveLogBuffer.computeIfAbsent(jobId, k -> Collections.synchronizedList(new LinkedList<>()));
        List<String> logs = liveLogBuffer.get(jobId);

        synchronized (logs) {
            logs.add(line);
            // Aggressively remove old logs from RAM
            while (logs.size() > MAX_LOG_LINES) {
                logs.remove(0);
            }
        }

        appendLogToDisk(jobId, line);
    }

    private void appendLogToDisk(String jobId, String line) {
        File directory = new File("titan_server_logs");
        if (!directory.exists()) {
            boolean created = directory.mkdirs(); // Force create the directory
            if (created) System.out.println("[INFO] Created log directory: titan_server_logs");
        }

        File logFile = new File(directory, jobId + ".log");

        try (FileWriter fw = new FileWriter("titan_server_logs/" + jobId + ".log", true)) {
            fw.write(line + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Helper method for the UI to retrieve the logs
//    public List<String> getLogs(String jobId) {
//        return liveLogBuffer.getOrDefault(jobId, new ArrayList<>());
//    }

    public List<String> getLogs(String jobId) {
        List<String> logs = liveLogBuffer.get(jobId);

        if (logs == null) {
            return new ArrayList<>();
        }

        // Return a COPY or Snapshot
        // This prevents ConcurrentModificationException when the UI loops over the logs
        // while the worker is simultaneously adding new ones.
        synchronized (logs) {
            return new ArrayList<>(logs);
        }
    }

    public String getSystemStats() {
        StringBuilder sb = new StringBuilder();
//        int activeCount = workerRegistry.getWorkerMap().size();
//        System.out.println("Active Workers: " + activeCount);
        sb.append("\n--- TITAN SYSTEM MONITOR ---\n");
        sb.append(String.format("Active Workers:    %d\n", workerRegistry.getWorkerMap().size()));
        sb.append(String.format("Execution Queue:   %d jobs\n", taskQueue.size()));
        sb.append(String.format("Delayed (Time):    %d jobs\n", waitingRoom.size()));
        sb.append(String.format("Blocked (DAG):     %d jobs\n", dagWaitingRoom.size()));
        sb.append(String.format("Dead Letter (DLQ): %d jobs\n", deadLetterQueue.size()));
        sb.append("-------------------------------\n");

        // Optional: List active workers and their current load
        if (!workerRegistry.getWorkers().isEmpty()) {
            sb.append("Worker Status:\n");
            for (Worker w : workerRegistry.getWorkers()) {
                int current = w.getCurrentLoad();
                int max = w.getMaxCap(); // Assuming your MAX_THREADS is 4, adjust as needed
                String loadStr = String.format("%d/%d (%d%%)", current, max, (current * 100 / max));

                sb.append(String.format(" • [%d] Load: %-12s | Skills: %s\n",
                        w.port(), loadStr, w.capabilities()));

                liveServiceMap.entrySet().stream()
                        .filter(entry -> entry.getValue().equals(w))
                        .forEach(entry -> {
                            String serviceId = entry.getKey();
                            // Optional: If you want to hide Child Workers from this list
                            // and only show them as main entries:
                            // if (serviceId.contains("worker")) return;

                            sb.append(String.format("    └── ⚙️ Service ID: %s\n", serviceId));
                        });
            }
        } else{
            sb.append("[INFO] No workers currently connected.\n");
        }
        return sb.toString();
    }

    public String getSystemStatsJSON() {
        StringBuilder json = new StringBuilder();
        json.append("{");

        List<Worker> safeWorkerList;
        synchronized (workerRegistry.getWorkers()) {
            safeWorkerList = new java.util.ArrayList<>(workerRegistry.getWorkers());
        }

        json.append("\"active_workers\": ").append(safeWorkerList.size()).append(",");
        json.append("\"queue_size\": ").append(taskQueue.size()).append(",");
        json.append("\"workers\": [");

        // Get the collection from your registry
        java.util.Collection<Worker> workers = workerRegistry.getWorkers();
        int workerCount = 0;
        int totalWorkers = workers.size();

        for (Worker w : safeWorkerList) {
            json.append("{");
            json.append("\"port\": ").append(w.port()).append(",");
            json.append("\"capabilities\": \"").append(w.capabilities()).append("\",");
            json.append("\"load\": \"").append(w.getCurrentLoad()).append("/").append(w.getMaxCap()).append("\",");

            if (w.currentJobId != null) {
                json.append("\"active_job\": \"").append(w.currentJobId).append("\", ");
            } else {
                json.append("\"active_job\": null, ");
            }

            json.append("\"history\": [");
            boolean hasRunningJob = false;

            if (w.currentJobId != null) {
                TaskExecution activeExec = executionHistory.get(w.currentJobId);
                if (activeExec != null) {
                    long duration = System.currentTimeMillis() - activeExec.startTime;
                    json.append(String.format("{\"id\": \"%s\", \"status\": \"RUNNING\", \"time\": \"%dms\"}",
                            w.currentJobId, duration));
                    hasRunningJob = true;
                }
            }

            String wKey = String.valueOf(w.port());
            java.util.Deque<Job> history = workerRecentHistory.get(wKey);

            // Check !isEmpty() to avoid printing a comma if there's no history to follow
            if (history != null && !history.isEmpty()) {
                // If we already printed a running job, we MUST add a comma before listing history
                if (hasRunningJob) {
                    json.append(",");
                }

                int hCount = 0;
                for (Job j : history) {
                    String duration = "N/A";
                    TaskExecution exec = executionHistory.get(j.getId());
                    if (exec != null) {
                        duration = exec.getDuration() + "ms";
                    }
                    json.append(String.format("{\"id\": \"%s\", \"status\": \"%s\", \"time\": \"%s\"}",
                            j.getId(), j.getStatus(), duration));
                    if (hCount < history.size() - 1) json.append(",");
                    hCount++;
                }
            }
            json.append("],");

            json.append("\"services\": [");

            // Filter liveServiceMap for keys (Service IDs) belonging to this worker
            java.util.List<String> services = liveServiceMap.entrySet().stream()
                    .filter(entry -> entry.getValue().equals(w))
                    .map(java.util.Map.Entry::getKey)
                    .toList();

            for (int j = 0; j < services.size(); j++) {
                json.append("\"").append(services.get(j)).append("\"");
                if (j < services.size() - 1) {
                    json.append(",");
                }
            }

            json.append("]}");

            // Add comma between worker objects, but not after the last one
            workerCount++;
            if (workerCount < totalWorkers) {
                json.append(",");
            }
        }

        json.append("]}");
        return json.toString();
    }

    public Job.Status getJobStatus(String id) {
        if (executionHistory.containsKey(id)) {
            return executionHistory.get(id).status;
        }
        return Job.Status.PENDING;
    }

    public void stop(){
        if(isRunning){
            isRunning = false;

            if (schedulerServer != null) schedulerServer.stop();

            serverExecutor.shutdownNow();
            heartBeatExecutor.shutdownNow();
            dispatchExecutor.shutdownNow();
        }
    }
}

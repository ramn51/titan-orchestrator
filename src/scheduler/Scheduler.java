package scheduler;

import network.RpcClient;
import network.SchedulerServer;
import network.TitanProtocol;

import java.io.File;
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
                    System.out.println("Time's up! Moving Job " + readyJob.getJob().getId() + " to Active Queue.");
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
            List<Worker> workers = new ArrayList<>(workerRegistry.getWorkers());
            if (workers.isEmpty()) {
                System.out.println("[SCALER] No workers found. Skipping...");
                return;
            }

            if (scalingInProgress) return;

            long busyCount = workers.stream().filter(w -> w.currentJobId != null).count();
            boolean allSaturated = workers.stream().allMatch(Worker::isSaturated);
            int totalCount = workers.size();
            int totalUsedSlots = workers.stream().mapToInt(Worker::getCurrentLoad).sum();
            int totalAvailableSlots = totalCount * 4;
            System.out.println("[SCALER] Cluster Pressure: " + totalUsedSlots + "/" + totalAvailableSlots);

            if (allSaturated && totalCount < MAX_WORKERS){
               scalingInProgress = true;
//               int nextPort = workers.stream().mapToInt(Worker::port).max().orElse(8080) + 1;
                // If the next port is not available then we just dont do scale up itself.
               int nextPort = findSafePort(workers);
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
            if (!allSaturated && totalCount > 1) {
                Worker idleTarget = workers.stream()
                        .filter(w -> w.port() != 8080) // Never kill the root
                        .filter(w -> w.getCurrentLoad() == 0) // Must be doing nothing
                        .filter(w -> w.getIdleDuration() > 45000) // Idle for > 45 seconds
                        .max(java.util.Comparator.comparingInt(Worker::port)) // Kill highest port first
                        .orElse(null);

                if (idleTarget != null) {
                    System.out.println("[SCALER] SCALE-DOWN: Worker " + idleTarget.port() + " is excess capacity. Removing.");
                    this.shutdownWorkerNode(idleTarget.port());
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
    private int findSafePort(List<Worker> currentWorkers) {
        int startPort = currentWorkers.stream().mapToInt(Worker::port).max().orElse(8080) + 1;
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
                workerRegistry.updateLastSeen(worker.host(), worker.port());
            }
        }
    }

    public Map<String, Worker> getLiveServiceMap(){
        return liveServiceMap;
    }

    public synchronized void registerWorker(String host, int port, String capability) {
        this.workerRegistry.addWorker(host, port, capability);
        this.scalingInProgress = false;
        this.portBlacklist.remove(port);

        System.out.println("[DEBUG] Attempting promotion for incoming worker at " + host + ":" + port);

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

        if (jobPayload.startsWith("DEPLOY_PAYLOAD") || jobPayload.startsWith("RUN_PAYLOAD")) {
            String temp = jobPayload.trim();
            long delay = 0;
            int priority = 1;
            //  Trying to extract DELAY from the end
            int lastPipe = temp.lastIndexOf('|');
            if (lastPipe != -1) {
                String suffix = temp.substring(lastPipe + 1);
                try {
                    // If the last part is a number, it's the DELAY, on success we strip it off.
                    delay = Long.parseLong(suffix);
                    temp = temp.substring(0, lastPipe);
                } catch (NumberFormatException e) {
                    // if its not a number Then it's part of the Base64 or filename.
                    // Delay remains 0. No stripping further.
                }
            }

            // Try to extract PRIORITY from the new end ---
            lastPipe = temp.lastIndexOf('|');
            if (lastPipe != -1) {
                String suffix = temp.substring(lastPipe + 1);
                try {
                    // If the new last part is a number, it's the PRIORITY and then on success we strip it off
                    priority = Integer.parseInt(suffix);
                    temp = temp.substring(0, lastPipe);
                } catch (NumberFormatException e) {
                    // Priority remains 1. No stripping further if its not a number
                }
            }
            // Submit the task 'temp' now contains just the raw payload (HEADER|FILE|BASE64)
            submitJob(new Job(temp, priority, delay));

        } else {

            String[] parts = jobPayload.split("\\|");
            String data = parts.length > 1 ? parts[0] + "|" + parts[1] : jobPayload;
            int priority = parts.length > 2 ? Integer.parseInt(parts[2]) : 1;
            long delay = parts.length > 3 ? Long.parseLong(parts[3]) : 0;

            submitJob(new Job(data, priority, delay));
        }
    }

    private void runDispatchLoop() throws InterruptedException {
        System.out.println("Running Dispatch Loop");
        while (isRunning) {
                Job job = taskQueue.take();
                System.out.println("DEBUG: Processing Job ID: " + job.getId());
                job.setStatus(Job.Status.RUNNING);
//                history.put(job.getId(), job.getStatus());
                System.out.println(" Job Processing: " + job);

                String[] parts = job.getPayload().split("\\|", 2);
                String rawHeader = parts[0];
                String reqTaskSkill = (rawHeader.equals("DEPLOY_PAYLOAD") || rawHeader.equals("RUN_PAYLOAD"))
                        ? "GENERAL"
                        : rawHeader;

                List<Worker> availableWorkers = workerRegistry.getWorkersByCapability(reqTaskSkill);

            if (availableWorkers.isEmpty() && !reqTaskSkill.equals("GENERAL")) {
                 System.out.println("DEBUG: No specialists for " + reqTaskSkill + ", trying GENERAL workers...");
                availableWorkers = workerRegistry.getWorkersByCapability("GENERAL");
            }

                if(availableWorkers.isEmpty()){
                    System.out.println("No available workers");
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

                System.out.println("[DAG] Setting Affinity for child " + waitingJob.getId() + " -> Worker " + workerPortId);
            }
        }
    }

    private String executeJobRequest(Job job, Worker worker) throws Exception {
        String rawPayload = job.getPayload();
        String actualPayload = rawPayload;
        worker.currentJobId = job.getId();

        boolean isSystemCommand = rawPayload.startsWith("DEPLOY_PAYLOAD") ||
                rawPayload.startsWith("RUN_PAYLOAD");

        if (!isSystemCommand && rawPayload.contains("|")) {
            // This handles "TEST|DataA" to "DataA"
            actualPayload = rawPayload.split("\\|", 2)[1];
        }

//        System.out.println("[DEBUG] Dispatching Clean Payload: " + actualPayload);

        if (job.getPayload().startsWith("DEPLOY_PAYLOAD")) {
            return executeDeploySequence(job, worker, actualPayload);
        } else if (job.getPayload().startsWith("RUN_PAYLOAD")) {
            return executeRunOneOff(job, worker, actualPayload);
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
            String[] parts = payload.split("\\|", 4);
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
                targetPort = Integer.parseInt(portString);
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
        String[] parts = payload.split("\\|", 3);
        String filename = parts[1];
        String base64Script = parts[2];

        // STEP 1: STAGE (Same as Deploy)
//        String stageResp = sendExecuteCommand(worker, TitanProtocol.OP_DEPLOY, "STAGE_FILE|" + filename + "|" + base64Script);
        String stageResp = sendExecuteCommand(worker, TitanProtocol.OP_STAGE, filename + "|" + base64Script);
        if (!stageResp.contains("FILE_SAVED")) {
            throw new RuntimeException("Staging failed: " + stageResp);
        }
        System.out.println("[OK] File Staged for Run");

        // Use Async Run Protocol (this will run the script as a background and send status to scheduler from worker)
        String payloadWithId = job.getId() + "|" + filename;
        return sendExecuteCommand(worker, TitanProtocol.OP_RUN, payloadWithId);

//        String runResp = sendExecuteCommand(worker, TitanProtocol.OP_RUN, filename);
//
//        // Worker returns: COMPLETED|0|Output...
//        if (!runResp.startsWith("COMPLETED")) {
//            throw new RuntimeException("Run failed: " + runResp);
//        }
//
//        return "RESULT: " + runResp;
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

    public String shutdownWorkerNode(int targetPort){
        Worker targetWorker = null;
        for(Worker w: this.getWorkerRegistry().getWorkers()){
            if(w.port() == targetPort){
                targetWorker = w;
                break;
            }
        }

        if(targetWorker == null){
            return "ERROR: Worker node " + targetPort + " not found in registry.";
        }

        System.out.println("[ADMIN] Initiating Graceful Shutdown for Worker " + targetPort);

        List<String> servicesToStop = new java.util.ArrayList<>();

        for(Map.Entry<String, Worker> entry: liveServiceMap.entrySet()){
            if(entry.getValue().equals(targetWorker)){
                servicesToStop.add(entry.getKey());
            }
        }

        for (String serviceId : servicesToStop) {
            System.out.println("[ADMIN] Stopping child service: " + serviceId);
            stopRemoteService(serviceId);
        }

        // Send the Kill Command to the Worker
        try {
            schedulerClient.sendRequest(targetWorker.host(), targetWorker.port(), TitanProtocol.OP_RUN, "SHUTDOWN_WORKER|NOW");
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
                // We pass null for the worker because it never left the scheduler.
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
                int max = 4; // Assuming your MAX_THREADS is 4, adjust as needed
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
            json.append("\"load\": \"").append(w.getCurrentLoad()).append("/4\",");

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

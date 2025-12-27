package scheduler;


import network.RpcClient;
import network.SchedulerServer;
import network.TitanProtocol;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.DelayQueue;

class ServiceInfo {
    Worker hostWorker;
    long startTime;
    String status; // "STARTING", "RUNNING", "FAILED"
}

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
    private final Map<String, Job.Status> history = new ConcurrentHashMap<>();
    private final Map<String, Worker> liveServiceMap = new ConcurrentHashMap<>();

    private final Map<String, Job> dagWaitingRoom;

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

        System.out.println("[DEBUG] Attempting promotion for incoming worker at " + host + ":" + port);

        liveServiceMap.entrySet().removeIf(entry -> {
            String serviceId = entry.getKey();
            Worker hostWorker = entry.getValue();

            // Debug prints to see why it's failing
            if (serviceId.startsWith("WRK-")) {
                System.out.println("[DEBUG] Checking Service: " + serviceId +
                        " | HostWorker Port: " + hostWorker.port() +
                        " | Target Port: " + port);
            }

            // The Fix: Only check the ID string, because the 'hostWorker'
            // in the map is the PARENT, not the new worker.
            boolean idMatches = serviceId.startsWith("WRK-" + port + "-");

            if (idMatches) {
                System.out.println("[INFO] Promotion Success: " + serviceId + " is now a Peer.");
                return true;
            }
            return false;
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
        String[] parts = jobPayload.split("\\|");
        String data = parts.length > 1 ? parts[0] + "|" + parts[1] : jobPayload;
        int priority = parts.length > 2 ? Integer.parseInt(parts[2]) : 1;
        long delay = parts.length > 3 ? Long.parseLong(parts[3]) : 0;

        submitJob(new Job(data, priority, delay));
    }

    private void runDispatchLoop() throws InterruptedException {
        System.out.println("Running Dispatch Loop");
        while (isRunning) {

//                if(taskQueue.isEmpty()){
//                    Thread.sleep(1000);
//                    continue;
//                }
                Job job = taskQueue.take();
                System.out.println("DEBUG: Processing Job ID: " + job.getId());
                job.setStatus(Job.Status.RUNNING);
                history.put(job.getId(), job.getStatus());
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

                if(bestWorker == null){
                    System.out.println("All workers SATURATED. Re-queueing job.");
                    job.setStatus(Job.Status.PENDING);
                    taskQueue.add(job);
                    history.put(job.getId(), job.getStatus());
                    Thread.sleep(1000);
                    continue;
                }

                Worker selectedWorker = bestWorker;
//                Worker selectedWorker = availableWorkers.get(ThreadLocalRandom.current().nextInt(availableWorkers.size()));
                System.out.println("[INFO] Dispatching " + job.getPayload() + " to Worker " + selectedWorker.port());

                try{
                        String response = executeJobRequest(job, selectedWorker);
                        System.out.println("[OK] Job Finished: " + response);
                        job.setStatus(Job.Status.COMPLETED);
                        history.put(job.getId(), job.getStatus());
                        unlockChildren(job.getId());

                } catch (Exception e){
                    handleJobFailure(job);
//                    history.put(job.getId(), job.getStatus());
                }
            }
        }

    private void handleJobFailure(Job job) {
        job.incrementRetry();
        if(job.getRetryCount() > 3) {
            job.setStatus(Job.Status.DEAD);
            history.put(job.getId(), Job.Status.DEAD);
            System.err.println("Job Moved to DLQ (Max Retries): " + job);
            this.deadLetterQueue.offer(job);
            cancelChildren(job.getId());

        } else{
            job.setStatus(Job.Status.FAILED);
            System.err.println("Job Failed. Retrying... (" + job.getRetryCount() + "/3)");
            job.setStatus(Job.Status.PENDING);
            history.put(job.getId(), Job.Status.PENDING);
            taskQueue.offer(job);
        }
    }

    private String executeJobRequest(Job job, Worker worker) throws Exception {
        if (job.getPayload().startsWith("DEPLOY_PAYLOAD")) {
            return executeDeploySequence(job, worker);
        } else if (job.getPayload().startsWith("RUN_PAYLOAD")) {
            return executeRunOneOff(job, worker);
        }
        else {
            return executeStandardTask(job, worker);
        }
    }

    private String executeStandardTask(Job job, Worker worker) throws Exception {
        return sendExecuteCommand(worker, TitanProtocol.OP_RUN, job.getPayload());
    }

    private String executeDeploySequence(Job job, Worker worker) throws Exception {
        String[] parts = job.getPayload().split("\\|", 4);
        String filename = parts[1];
        String base64Script = parts[2];

        System.out.println("SCHEDULER LOGS::ARGS PASSED TO DEPLOY EXEC " + parts.length);

        // This will be passed only for the jar based deployment and not for python ones.
        String optionalPort = (parts.length > 3) ? parts[3] : "8085";

        // Step 1: Stage
        String stagePayload = filename + "|" + base64Script;
        String stageResp = sendExecuteCommand(worker, TitanProtocol.OP_STAGE, stagePayload);
        if (!stageResp.contains("FILE_SAVED")) {
            throw new RuntimeException("Staging failed. Expected FILE_SAVED, got: " + stageResp);
        }
        System.out.println("[OK] File Staged");

        // Step 2: Start
        String startPayload = filename + "|" + job.getId() + "|" + optionalPort;

        String startResp = sendExecuteCommand(worker, TitanProtocol.OP_START_SERVICE, startPayload);
        if (!startResp.contains("DEPLOYED_SUCCESS")) {
            throw new RuntimeException("Start failed. Expected DEPLOYED_SUCCESS, got: " + startResp);
        }

        String pid = startResp.contains("PID:") ? startResp.split("PID:")[1].trim() : "UNKNOWN";
        liveServiceMap.put(job.getId(), worker);
        return "DEPLOYED_SUCCESS PID:" + pid;
    }

    private String executeRunOneOff(Job job, Worker worker) throws Exception {
        String[] parts = job.getPayload().split("\\|", 3);
        String filename = parts[1];
        String base64Script = parts[2];

        // STEP 1: STAGE (Same as Deploy)
//        String stageResp = sendExecuteCommand(worker, TitanProtocol.OP_DEPLOY, "STAGE_FILE|" + filename + "|" + base64Script);
        String stageResp = sendExecuteCommand(worker, TitanProtocol.OP_STAGE, filename + "|" + base64Script);
        if (!stageResp.contains("FILE_SAVED")) {
            throw new RuntimeException("Staging failed: " + stageResp);
        }
        System.out.println("[OK] File Staged for Run");

        // STEP 2: RUN AND WAIT
        // Protocol: EXECUTE RUN_SCRIPT|filename
        String runResp = sendExecuteCommand(worker, TitanProtocol.OP_RUN, filename);

        // Worker returns: COMPLETED|0|Output...
        if (!runResp.startsWith("COMPLETED")) {
            throw new RuntimeException("Run failed: " + runResp);
        }

        return "RESULT: " + runResp;
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
                history.put(job.getId(), Job.Status.DEAD);
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
        return history.getOrDefault(id, Job.Status.PENDING);
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

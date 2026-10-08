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



package titan.scheduler;

import titan.filesys.AssetManager;
import titan.network.TitanProtocol;
import titan.network.RpcClient;
import titan.network.SchedulerServer;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.DelayQueue;
import titan.TitanConfig;
import titan.storage.TitanJRedisAdapter;

/**
 * The main Scheduler class responsible for managing workers, dispatching jobs, and maintaining system state.
 * It handles job submission, scheduling, execution, and recovery, interacting with workers via RPC and
 * persisting state using Redis.
 */
    public class Scheduler {
    private final WorkerRegistry workerRegistry;
    private final RpcClient schedulerClient;
//    private final Queue<Job> taskQueue;
    private final BlockingQueue<Job> taskQueue;
    private final BlockingQueue<ScheduledJob> waitingRoom;
    private final Queue<Job> deadLetterQueue;
    private final SchedulerServer schedulerServer;

    private final ScheduledExecutorService heartBeatExecutor;
    private final ScheduledExecutorService telemetryExecutor;
    private final ExecutorService dispatchExecutor;
    /** One retained wake-up is enough: a single dispatcher rechecks all worker capacity. */
    private final BlockingQueue<Boolean> capacitySignal = new ArrayBlockingQueue<>(1);
    private final ExecutorService serverExecutor;
    private volatile boolean isRunning = true;
    private int port;

    private int redisPort = TitanConfig.getInt("titan.redis.port", 6379);
    private String redisHost = TitanConfig.get("titan.redis.host", "localhost");


    // JRedis config
    private final TitanJRedisAdapter redis;

    // This is purely for validation purpose
    private final Map<String, TaskExecution> executionHistory = new ConcurrentHashMap<>();
    private final ExecutionTimeline executionTimeline = new ExecutionTimeline();
    private final SchedulerTelemetry telemetry = new SchedulerTelemetry();
    private final Map<String, Long> readySince = new ConcurrentHashMap<>();
    private final Map<String, Job> capabilityWaiting = new ConcurrentHashMap<>();
    private final Map<String, String> waitingReasons = new ConcurrentHashMap<>();
    private final Map<String, Worker> liveServiceMap = new ConcurrentHashMap<>();
    public record ServiceEndpoint(String host, int port) {}
    private final Map<String, ServiceEndpoint> serviceEndpoints = new ConcurrentHashMap<>();
    private final Map<String, ServiceEndpoint> pendingServiceEndpoints = new ConcurrentHashMap<>();
    private final Set<String> stoppingServices = ConcurrentHashMap.newKeySet();
    private final ServiceReadinessMonitor readinessMonitor;

    // For history maintenance of (tasks all types).
    private final Map<String, Integer> workerCompletionStats = new ConcurrentHashMap<>();
    private final Map<String, java.util.Deque<Job>> workerRecentHistory = new ConcurrentHashMap<>();

    // Map to hold Active Job Objects for Async Retries
    private final Map<String, Job> runningJobs = new ConcurrentHashMap<>();
    private final Map<String, Integer> heartbeatMisses = new ConcurrentHashMap<>();
    private final Map<String, Job> knownJobs = new ConcurrentHashMap<>();
    private final Map<String, Job> dagWaitingRoom;

    // AutoScaling declarations
    private final int maxWorkers = Math.max(1, TitanConfig.getInt("titan.scheduler.max.workers", 5));
    private final boolean autoscaleEnabled = Boolean.parseBoolean(
            TitanConfig.get("titan.scheduler.autoscale", "true"));
    private volatile boolean scalingInProgress = false;
    private final ScheduledExecutorService scalerExecutor = Executors.newSingleThreadScheduledExecutor();
    // Remember bad ports for avoiding during scaling
    private final Set<Integer> portBlacklist = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // This is related to log streaming
    private final Map<String, List<String>> liveLogBuffer = new ConcurrentHashMap<>();
    private static final int MAX_LOG_LINES = 100;

    /**
 * Constructs a new Scheduler instance, initializing its core components.
 * This includes the worker registry, RPC client, job queues (task, dead-letter, waiting room, DAG waiting room),
 * and executor services for heartbeats, dispatching, and the scheduler server.
 * It also sets up the Redis adapter for persistence and starts a clock watcher thread for delayed jobs.
 *
 * @param port The port on which the scheduler server will listen for incoming requests.
 * @throws RuntimeException if the Scheduler Server fails to start due to an IOException.
 */
    public Scheduler(int port){
        this(port, new ServiceReadinessMonitor(), null);
    }

    Scheduler(int port, ServiceReadinessMonitor readinessMonitor){
        this(port, readinessMonitor, new TitanJRedisAdapter("", 0));
    }

    private Scheduler(int port, ServiceReadinessMonitor readinessMonitor,
                      TitanJRedisAdapter testRedis){
        this.readinessMonitor = readinessMonitor;
        workerRegistry = new WorkerRegistry();
        schedulerClient = new RpcClient(workerRegistry);
//        this.taskQueue = new ConcurrentLinkedDeque<>();
        this.taskQueue = new PriorityBlockingQueue<>();
        this.deadLetterQueue = new ConcurrentLinkedDeque<>();
        this.waitingRoom = new DelayQueue<>();
        this.dagWaitingRoom = new ConcurrentHashMap<>();

        this.port = port;
        this.heartBeatExecutor = Executors.newSingleThreadScheduledExecutor();
        this.telemetryExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "titan-telemetry-sampler");
            thread.setDaemon(true);
            return thread;
        });
        this.dispatchExecutor = Executors.newSingleThreadExecutor();
        this.serverExecutor = Executors.newSingleThreadExecutor();

        String rHost = TitanConfig.get("titan.redis.host", "localhost");
        int rPort = TitanConfig.getInt("titan.redis.port", 6379);
        this.redis = testRedis == null ? new TitanJRedisAdapter(redisHost, redisPort) : testRedis;

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
                    synchronized (Scheduler.this) {
                        if (readyJob.getJob().getStatus() == Job.Status.PENDING) {
                            readySince.put(readyJob.getJob().getId(), System.currentTimeMillis());
                            taskQueue.add(readyJob.getJob());
                        }
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        clockWatcher.setDaemon(true);
        clockWatcher.start();
    }

    /**
 * Initiates the scheduler's operations. This method connects to Redis, attempts to recover any orphaned jobs,
 * starts the scheduler server in a separate thread, and schedules periodic heartbeat checks for workers.
 * It also starts the main job dispatch loop.
 */
    public void start(){
        System.out.println("Scheduler Core starting at port " + this.port);
        try {
            redis.connect();
            if(redis.isConnected()){
                System.out.println("[INFO] Redis Persistence Layer Active");
                // Perform State recovery once connected for failed jobs
                recoverState();
            }
        } catch (IOException e) {
            System.err.println("[INFO][FAILED] Redis connection failed: " + e.getMessage());
        }

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
        telemetryExecutor.scheduleAtFixedRate(this::sampleTelemetry, 1,
                Math.max(1, TitanConfig.getInt("titan.observability.sample.seconds", 1)), TimeUnit.SECONDS);

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

    /**
 * Retrieves the Redis adapter instance used by the scheduler.
 *
 * @return The {@link TitanJRedisAdapter} instance.
 */
    public TitanJRedisAdapter getRedis(){
        return this.redis;
    }

    /**
 * Retrieves the worker registry instance used by the scheduler.
 *
 * @return The {@link WorkerRegistry} instance.
 */
    public WorkerRegistry getWorkerRegistry(){
        return workerRegistry;
    }

    // AutoScale methods and helpers

    /**
 * Activates the auto-scaling mechanism for the Titan cluster.
 * This method schedules a periodic task to reconcile the cluster size based on worker load and availability.
 */
    public void startAutoScaler(){
        System.out.println("[INFO] Titan Auto-Scaler active.");
        scalerExecutor.scheduleAtFixedRate(this::reconcileClusters, 15, 15, TimeUnit.SECONDS);
    }

    /**
 * Periodically checks the cluster's health and load to determine if scaling actions (up or down) are needed.
 * It identifies saturated worker pools to scale up new workers and detects idle, non-permanent workers for scale-down.
 * Scaling up involves submitting a special 'DEPLOY_PAYLOAD' job to launch a new worker.
 * Scaling down involves gracefully shutting down an idle worker node.
 */
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

            if (autoscaleEnabled && generalPoolSaturated && totalCount < maxWorkers){
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
               telemetry.event("SCALE_OUT", "GENERAL worker requested on port " + nextPort);
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

            if (autoscaleEnabled && !generalPoolSaturated && totalCount > 1) {
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
                    telemetry.event("SCALE_IN", idleTarget.host() + ":" + idleTarget.port());
                    this.shutdownWorkerNode(idleTarget.host(), idleTarget.port());
                    // 2. Remove from local registry immediately
                    workerRegistry.getWorkerMap().remove(idleTarget.host() + ":" + idleTarget.port());
                    schedulerClient.evict(idleTarget.host(), idleTarget.port());
                }
            }
        } catch (Exception e) {
            System.err.println("[SCALER ERROR] " + e.getMessage());
            this.scalingInProgress = false;
        }
    }

    // Helper methods related to scaling. Finding the available ports for new spawning.
    /**
 * Finds an available port within a specified range for a new worker.
 * It checks against currently registered workers and a blacklist of recently failed ports.
 *
 * @param currentWorkers A list of currently active workers.
 * @param minRange The minimum port number to consider.
 * @param maxRange The maximum port number to consider.
 * @return An available port number, or -1 if no safe port is found within the scan range.
 */
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

    /**
 * Checks if a given port is currently in use on the local machine.
 *
 * @param port The port number to check.
 * @return {@code true} if the port is in use, {@code false} otherwise.
 */
    private boolean isPortInUseLocally(int port) {
        try (Socket ignored = new Socket("localhost", port)) {
            return true;
        } catch (IOException e) {
            return false; // Connection refused = Port is free
        }
    }

    /**
 * Sends heartbeat requests to all registered workers to verify their liveness and update their load status.
 * Workers that do not respond are marked as dead. Responding workers update their last seen timestamp and load metrics.
 * This method also updates Redis with the status of live workers.
 */
    public void checkHeartBeat(){
        System.out.println("Sending Heartbeat");
        for(Worker worker: workerRegistry.getWorkers()){
            long heartbeatStarted = System.nanoTime();
            String result = schedulerClient.sendRequest(
                    worker.host(),
                    worker.port(),
                    TitanProtocol.OP_HEARTBEAT,
                    ""
            );

            if(result  == null){
                telemetry.heartbeat(false);
                String workerKey = worker.host() + ":" + worker.port();
                if (heartbeatMisses.merge(workerKey, 1, Integer::sum) < 2) {
                    System.err.println("[HEARTBEAT] First miss for " + workerKey + "; checking again before recovery");
                    continue;
                }
                heartbeatMisses.remove(workerKey);
                telemetry.event("WORKER_LOST", worker.host() + ":" + worker.port());
                workerRegistry.markWorkerDead(worker.host(), worker.port());
                schedulerClient.evict(worker.host(), worker.port());
                recoverJobsForLostWorker(worker);
                for (Map.Entry<String, Worker> service : new ArrayList<>(liveServiceMap.entrySet())) {
                    if (service.getValue() == worker) unregisterService(service.getKey());
                }
                for (String serviceId : new ArrayList<>(pendingServiceEndpoints.keySet())) {
                    TaskExecution record = executionHistory.get(serviceId);
                    if (record != null && record.assignedWorker == worker) unregisterService(serviceId);
                }
            } else if(result.startsWith("PONG")){
                heartbeatMisses.remove(worker.host() + ":" + worker.port());
                telemetry.heartbeat(true);
                worker.updateHeartbeatRtt(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - heartbeatStarted));
                String workerKey = worker.host() + ":" + worker.port();

                safeRedisSadd("system:live_workers", workerKey);

                worker.updateLastSeen();
                String[] parts = result.split("\\|");
                if (parts.length > 1) {
                    int load = Integer.parseInt(parts[1]);
                    long assigned = runningJobs.keySet().stream()
                            .map(executionHistory::get)
                            .filter(record -> record != null && record.assignedWorker == worker)
                            .count();
                    int previousLoad = worker.getCurrentLoad();
                    worker.reconcileLoad(load, (int) assigned);
                    if (worker.getCurrentLoad() < previousLoad) signalCapacityAvailable();

                    safeRedisSet("worker:" + workerKey + ":load", String.valueOf(worker.getCurrentLoad()));

                    if(load > 0){
                        System.out.println("Worker " + worker.port() + "Has load" + worker.getCurrentLoad());
                    }
                }
                if (parts.length > 2) {
                    int maxCapacity = Integer.parseInt(parts[2]);
                    worker.setMaxCap(maxCapacity);
                }
                if (parts.length > 5) {
                    try {
                        worker.updateHostMetrics(Double.parseDouble(parts[3]),
                                Long.parseLong(parts[4]), Long.parseLong(parts[5]));
                    } catch (NumberFormatException ignored) { }
                }
                if (parts.length > 6) {
                    try { worker.updateHostLoadAverage(Double.parseDouble(parts[6])); }
                    catch (NumberFormatException ignored) { }
                }
                workerRegistry.updateLastSeen(worker.host(), worker.port());
            }
        }
    }

    /**
 * Retrieves the map of currently running services (identified by job ID) to their assigned worker.
 *
 * @return A map where keys are service job IDs and values are the {@link Worker} instances hosting them.
 */
    public Map<String, Worker> getLiveServiceMap(){
        return liveServiceMap;
    }

    /** Retire every in-flight attempt owned by a lost worker before it can be retried elsewhere. */
    synchronized void recoverJobsForLostWorker(Worker worker) {
        for (Map.Entry<String, Job> entry : new ArrayList<>(runningJobs.entrySet())) {
            String jobId = entry.getKey();
            Job job = entry.getValue();
            TaskExecution record = executionHistory.get(jobId);
            if (record == null || record.assignedWorker != worker || job.getStatus() == Job.Status.CANCELLED
                    || !runningJobs.remove(jobId, job)) continue;
            String reason = "Assigned worker lost heartbeat: " + worker.host() + ":" + worker.port();
            record.fail(reason);
            telemetry.failed();
            executionTimeline.finish(jobId, "FAILED", reason);
            recordWorkerHistory(job, record);
            worker.decrementCurrentLoad();
            worker.currentJobId = null;
            handleJobFailure(job);
            signalCapacityAvailable();
        }
    }

    public String resolveService(String serviceId) {
        ServiceEndpoint endpoint = serviceEndpoints.get(serviceId);
        return endpoint == null ? "ERROR: Service not found" : endpoint.host() + "|" + endpoint.port();
    }

    public void unregisterService(String serviceId) {
        boolean cancelDependents = false;
        synchronized (this) {
            stoppingServices.remove(serviceId);
            liveServiceMap.remove(serviceId);
            serviceEndpoints.remove(serviceId);
            if (pendingServiceEndpoints.remove(serviceId) != null) {
                readinessMonitor.cancel(serviceId);
                Job job = runningJobs.remove(serviceId);
                if (job != null && job.getStatus() != Job.Status.CANCELLED) {
                    String reason = "Service process exited before its port became ready";
                    TaskExecution record = executionHistory.get(serviceId);
                    record.fail(reason);
                    telemetry.failed();
                    executionTimeline.finish(serviceId, "FAILED", reason);
                    job.setStatus(Job.Status.FAILED);
                    safeRedisSet("job:" + serviceId + ":status", "FAILED");
                    safeRedisSrem("system:active_jobs", serviceId);
                    if (record.assignedWorker != null) {
                        record.assignedWorker.currentJobId = null;
                        record.assignedWorker.decrementCurrentLoad();
                        signalCapacityAvailable();
                    }
                    recordWorkerHistory(job, record);
                    cancelDependents = true;
                }
            }
        }
        if (cancelDependents) cancelChildren(serviceId);
    }

    public void clearRuntimeStats() {
        workerCompletionStats.clear();
        workerRecentHistory.clear();
    }

    /**
 * Registers a new worker with the scheduler. This method adds the worker to the registry,
 * clears any `scalingInProgress` flag, and removes the port from the blacklist.
 * Deployment readiness callbacks release the parent worker's dispatch slot.
 *
 * @param host The hostname or IP address of the worker.
 * @param port The port number the worker is listening on.
 * @param capability A string describing the worker's capabilities (e.g., "GENERAL", "GPU").
 * @param isPermanent {@code true} if the worker is a permanent part of the cluster and should not be scaled down, {@code false} otherwise.
 */
    public synchronized void registerWorker(String host, int port, String capability, boolean isPermanent) {
        String workerKey = workerRegistry.getWorkerKey(host, port);
        boolean firstRegistration = !workerRegistry.getWorkerMap().containsKey(workerKey);
        this.workerRegistry.addWorker(host, port, capability, isPermanent);
        for (Job parked : new ArrayList<>(capabilityWaiting.values())) {
            if (workerRegistry.getWorkersByCapability(extractSkillRequirement(parked)).isEmpty()) continue;
            if (capabilityWaiting.remove(parked.getId(), parked) && parked.getStatus() == Job.Status.PENDING) {
                waitingReasons.remove(parked.getId());
                taskQueue.add(parked);
            }
        }
        // Workers re-register periodically for recovery. Recording each refresh
        // would evict actual scale and failure events from the bounded journal.
        if (firstRegistration)
            telemetry.event("WORKER_REGISTERED", host + ":" + port + " " + capability);
        signalCapacityAvailable();
        this.scalingInProgress = false;
        this.portBlacklist.remove(port);

//        System.out.println("[DEBUG] Attempting promotion for incoming worker at " + host + ":" + port);
        if (firstRegistration)
            System.out.println("[INFO] New Worker Registered: " + host + ":" + port +
                    (isPermanent ? " [PERMANENT]" : " [EPHEMERAL]"));

        // The deployment readiness callback releases the parent worker's slot and
        // keeps the service endpoint registered even when the child is itself a worker.
    }

    /**
 * Retrieves the map of jobs currently waiting for their DAG dependencies to be met.
 *
 * @return A map where keys are job IDs and values are the {@link Job} instances waiting on dependencies.
 */
    public Map<String, Job> getDAGWaitingRoom(){
        return dagWaitingRoom;
    }

    /** Register the whole DAG before admitting any node, including on redeploy. */
    public synchronized void submitDag(List<Job> jobs) {
        Set<String> ids = new HashSet<>();
        for (Job job : jobs) {
            if (!safeJobId(job.getId()) || !ids.add(job.getId())) {
                throw new IllegalArgumentException("Invalid or duplicate DAG job ID");
            }
        }
        for (Job job : jobs) knownJobs.put(job.getId(), job);
        for (Job job : jobs) submitJob(job);
    }

    /**
 * Submits a {@link Job} to the scheduler for processing. The job's state is persisted to Redis.
 * If the job has dependencies, it's placed in the DAG waiting room. If it has a scheduled time in the future,
 * it's placed in the waiting room. Otherwise, it's added directly to the active task queue.
 *
 * @param job The {@link Job} object to be submitted.
 */
    public synchronized void submitJob(Job job){
        if (!safeJobId(job.getId())) throw new IllegalArgumentException("Invalid job ID");
        String payload = job.getPayload();
        if (payload != null && (payload.contains("DEPLOY_PAYLOAD|")
                || payload.contains("START_ARCHIVE_SERVICE|"))
                && (liveServiceMap.containsKey(job.getId()) || pendingServiceEndpoints.containsKey(job.getId())
                || stoppingServices.contains(job.getId()))) {
            throw new IllegalArgumentException("Deployment Rejected: Service ID already in use: " + job.getId());
        }
        knownJobs.put(job.getId(), job);
        liveLogBuffer.remove(job.getId());
        // Persist to Redis to act as WAL
        // This will be the basis for recovery
        safeRedisSet("job:" + job.getId() + ":payload", job.getPayload());
        safeRedisSet("job:" + job.getId() + ":status", "PENDING");
        safeRedisSet("job:" + job.getId() + ":priority", String.valueOf(job.getPriority()));
        safeRedisSet("job:" + job.getId() + ":delay", String.valueOf(job.getScheduledTime()));
        safeRedisSet("job:" + job.getId() + ":dependencies", String.join(",", job.getDependenciesIds()));
        safeRedisSadd("system:active_jobs", job.getId());

        // A parent can finish before its child is submitted. The same monitor guards
        // registration and unlockChildren, so no completion can pass between the
        // final check and insertion into the waiting room.
        resolveCompletedParents(job);
        if (!job.isReady()) {
            dagWaitingRoom.put(job.getId(), job);
            resolveCompletedParents(job);
            if (!job.isReady() || !dagWaitingRoom.remove(job.getId(), job)) return;
        }

        enqueueReadyJob(job);
    }

    private void resolveCompletedParents(Job job) {
        for (String parentId : job.getDependenciesIds()) {
            Job parent = knownJobs.get(parentId);
            if (parent != null && parent.getStatus() == Job.Status.COMPLETED) {
                job.resolveDependencies(parentId);
            }
        }
    }

    private void enqueueReadyJob(Job job) {
        long delay = job.getScheduledTime() - System.currentTimeMillis();
        if(delay <=0){
            // Run the job now (Add to the queue, dispatcher will do the polling and execution)
            System.out.println(" ** Queueing Job: " + job.getId());
            readySince.put(job.getId(), System.currentTimeMillis());
            taskQueue.add(job);
        } else{
            System.out.println("[INFO] Job Delayed: " + job.getId() + " for " + delay + "ms");
            waitingRoom.add(new ScheduledJob(job));
        }
    }

    /**
 * Submits a job to the scheduler using a raw string payload. This method parses the payload
 * to extract job ID, priority, and delay, then constructs a {@link Job} object and delegates
 * to {@link #submitJob(Job)}.
 * The payload format can include optional ID, priority, and delay separated by pipes.
 * Example: "JOB-101|RUN_PAYLOAD|script.py|data|GPU|10|1000" (ID|Payload|Priority|Delay)
 *
 * @param jobPayload The raw string payload representing the job.
 */
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

    /**
 * Extracts the skill requirement (capability) from a job's payload.
 * This method parses the payload string to identify specific keywords or patterns
 * that indicate a worker capability needed for the job (e.g., "GPU", "GENERAL").
 * It handles various payload formats and metadata to accurately determine the skill.
 *
 * @param job The {@link Job} for which to extract the skill requirement.
 * @return A string representing the required skill (e.g., "GPU", "GENERAL"), defaulting to "GENERAL" if none is found.
 */
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

    /**
 * Sets a key-value pair in Redis. This is a public wrapper around {@link #safeRedisSet(String, String)}.
 *
 * @param key The key to set.
 * @param value The value to associate with the key.
 */
    public void redisKVSet(String key, String value){
        safeRedisSet(key, value);
    }

    /**
 * Adds a member to a Redis set. This is a public wrapper around {@link #safeRedisSadd(String, String)}.
 *
 * @param key The key of the set.
 * @param value The member to add to the set.
 */
    public void redisSetAdd(String key, String value){
        safeRedisSadd(key, value);
    }

    /**
 * Retrieves the value associated with a given key from Redis.
 *
 * @param key The key to retrieve.
 * @return The string value associated with the key, or {@code null} if the key does not exist or an error occurs.
 */
    public String redisKVGet(String key){
        long started = System.nanoTime();
        try {
            return redis.get(key);
        } catch (Exception e) {
            System.err.println("[INFO][FAILED][RECOVERY] Failed to fetch active jobs: " + e.getMessage());
            return null;
        } finally {
            telemetry.storeRead(System.nanoTime() - started);
        }
    }

    /**
 * Safely sets a key-value pair in Redis, catching and logging any IOException.
 *
 * @param key The key to set.
 * @param value The value to associate with the key.
 */
    private void safeRedisSet(String key, String value) {
        long started = System.nanoTime();
        boolean succeeded = false;
        try {
            redis.set(key, value);
            succeeded = true;
        } catch (IOException e) {
            System.err.println("[WARN] Redis SET failed for " + key + ": " + e.getMessage());
        } finally {
            telemetry.storeWrite(System.nanoTime() - started, succeeded);
        }
    }

    // Methods related to Redis for persistance
    /**
 * Safely adds a member to a Redis set, catching and logging any IOException.
 *
 * @param key The key of the set.
 * @param member The member to add to the set.
 */
    private void safeRedisSadd(String key, String member) {
        long started = System.nanoTime();
        boolean succeeded = false;
        try {
            redis.sadd(key, member);
            succeeded = true;
        } catch (IOException e) {
            System.err.println("[WARN] Redis SADD failed for " + key + ": " + e.getMessage());
        } finally {
            telemetry.storeWrite(System.nanoTime() - started, succeeded);
        }
    }

    /**
 * Safely removes a member from a Redis set, catching and logging any exceptions.
 *
 * @param key The key of the set.
 * @param member The member to remove from the set.
 */
    private void safeRedisSrem(String key, String member) {
        long started = System.nanoTime();
        boolean succeeded = false;
        try {
            redis.srem(key, member);
            succeeded = true;
        } catch (Exception e) { // Catch Exception broadly as srem isn't in interface yet maybe
            System.err.println("[WARN] Redis SREM failed for " + key + ": " + e.getMessage());
        } finally {
            telemetry.storeWrite(System.nanoTime() - started, succeeded);
        }
    }

    /**
 * Safely retrieves all members of a Redis set, catching and logging any exceptions.
 *
 * @param key The key of the set.
 * @return A {@link Set} of strings representing the members of the set, or {@code null} if an error occurs.
 */
    public Set<String> safeRedisSMembers(String key) {
        try {
            return redis.smembers(key);
        } catch (Exception e) { // Catch Exception broadly as srem isn't in interface yet maybe
            System.err.println("[WARN] Redis SREM failed for " + key + ": " + e.getMessage());
            return null;
        }
    }

    /**
 * Recovers the scheduler's state from Redis upon startup. It scans for active jobs
 * that were not marked as completed or dead, and re-queues them into the appropriate
 * scheduler queues (waiting room or task queue) based on their status and scheduled time.
 * This ensures job continuity across scheduler restarts.
 */
    private void recoverState(){
        if(!redis.isConnected()){
            System.out.println("[INFO][WARN] Redis not connected");
            return;
        }

        System.out.println("[INFO][RECOVERY] Scanning for orphaned jobs...");
        Set<String> activeIds = Collections.emptySet();
        try {
            activeIds = redis.smembers("system:active_jobs");
        } catch (Exception e) {
            System.err.println("[INFO][FAILED][RECOVERY] Failed to fetch active jobs: " + e.getMessage());
            return;
        }

        if (activeIds.isEmpty()) {
            System.out.println("[INFO][RECOVERY] No stranded jobs found.");
            return;
        }

        int recoveredCount = 0;

        for(String jobId: activeIds){
            try{
                String status = redis.get("job:" + jobId + ":status");
                String payload = redis.get("job:" + jobId + ":payload");
                if (payload == null || status == null) {
                    System.err.println("[INFO][ERROR][RECOVERY] Corrupt job found: " + jobId + ". Removing.");
                    safeRedisSrem("system:active_jobs", jobId);
                    continue;
                }

                String priStr = redis.get("job:" + jobId + ":priority");
                String delayStr = redis.get("job:" + jobId + ":delay");
                int priority = (priStr != null) ? Integer.parseInt(priStr) : 1;
                long scheduledTime = (delayStr != null) ? Long.parseLong(delayStr) : 0;

                // Calculate remaining delay (if it was a future job)
                long remainingDelay = Math.max(0, scheduledTime - System.currentTimeMillis());

                String dependencies = redis.get("job:" + jobId + ":dependencies");
                List<String> parentIds = dependencies == null || dependencies.isBlank()
                        ? Collections.emptyList() : Arrays.asList(dependencies.split(","));
                Job job = new Job(jobId, payload, priority, remainingDelay, parentIds);
                job.setStatus(Job.Status.PENDING); // Force reset to Pending

                if ("COMPLETED".equals(status) || "DEAD".equals(status) || "CANCELLED".equals(status)) {
                    // Should have been removed, but if it's here, clean it.
                    safeRedisSrem("system:active_jobs", jobId);
                } else {
                    System.out.println("[INFO][RECOVERY] Restoring Job " + jobId + " (Was " + status + ")");
                    // We bypass submitJob() to avoid writing to Redis again needlessly but we need the queue logic.
                    knownJobs.put(jobId, job);
                    for (String parentId : parentIds) {
                        if ("COMPLETED".equals(redis.get("job:" + parentId + ":status")))
                            job.resolveDependencies(parentId);
                    }
                    if (!job.isReady()) {
                        dagWaitingRoom.put(jobId, job);
                    } else if (remainingDelay > 0) {
                        waitingRoom.add(new ScheduledJob(job));
                    } else {
                        taskQueue.add(job);
                    }
                    recoveredCount++;
                }

            } catch (Exception e) {
                System.err.println("[ERROR][RECOVERY] Error restoring " + jobId + ": " + e.getMessage());
            }
        }
        for (Job job : new ArrayList<>(knownJobs.values())) {
            try {
                for (String parentId : job.getDependenciesIds()) {
                    String parentStatus = redis.get("job:" + parentId + ":status");
                    if ("CANCELLED".equals(parentStatus) || "DEAD".equals(parentStatus)) {
                        cancelJob(job.getId());
                        break;
                    }
                }
            } catch (IOException error) {
                System.err.println("[RECOVERY] Could not inspect parents of " + job.getId());
            }
        }
        System.out.println("[INFO][RECOVERY] Complete. Restored " + recoveredCount + " jobs.");
    }

    /**
 * The main dispatch loop of the scheduler. This loop continuously polls the task queue for new jobs.
 * When a job is available, it determines the required skill, selects the best available worker,
 * dispatches the job to that worker, and handles the job's execution and potential failures.
 * If no suitable worker is found or all workers are saturated, the job is re-queued.
 *
 * @throws InterruptedException If the dispatch loop thread is interrupted.
 */
    private void runDispatchLoop() throws InterruptedException {
        System.out.println("Running Dispatch Loop");
        while (isRunning) {
                Job job = taskQueue.take();
                long cycleStarted = System.nanoTime();
                synchronized (this) {
                    if (job.getStatus() == Job.Status.CANCELLED) continue;
                    job.setStatus(Job.Status.RUNNING);
                }
                System.out.println("DEBUG: Processing Job ID: " + job.getId());
//                history.put(job.getId(), job.getStatus());
                System.out.println(" Job Processing: " + job);

                String reqTaskSkill = extractSkillRequirement(job);

                System.out.println("[DISPATCH] Job " + job.getId() + " requires: [" + reqTaskSkill + "]");

                List<Worker> availableWorkers = workerRegistry.getWorkersByCapability(reqTaskSkill);
                long routedAt = System.nanoTime();

                if (availableWorkers.isEmpty()) {
                    if (!reqTaskSkill.equals("GENERAL")) {
                        System.out.println("[WAIT] No active workers found with capability: " + reqTaskSkill);
                    } else {
                        System.out.println("[WAIT] No GENERAL workers available.");
                    }

                    // Park until a matching worker registers; preserve eligibility time.
                    synchronized (this) {
                        if (job.getStatus() != Job.Status.CANCELLED) {
                            job.setStatus(Job.Status.PENDING);
                            capabilityWaiting.put(job.getId(), job);
                            waitingReasons.put(job.getId(), "No worker has capability " + reqTaskSkill);
                        }
                    }
                    continue;
                }

                Worker selectedWorker = selectAndReserveWorker(job, availableWorkers);
                long selectedAt = System.nanoTime();

                if (selectedWorker == null) {
                    System.out.println("All workers SATURATED or unavailable. Re-queueing job.");
                    synchronized (this) {
                        if (job.getStatus() != Job.Status.CANCELLED) {
                            job.setStatus(Job.Status.PENDING);
                            taskQueue.put(job);
                            waitingReasons.put(job.getId(), "Matching workers are at capacity");
                        }
                    }
                    awaitCapacityChange();
                    continue;
                }
                TaskExecution record;
                long recordedAt;
                long storedAt;
                long eligibleAt = readySince.getOrDefault(job.getId(), System.currentTimeMillis());
                synchronized (this) {
                    if (job.getStatus() == Job.Status.CANCELLED) {
                        selectedWorker.decrementCurrentLoad();
                        signalCapacityAvailable();
                        continue;
                    }
                    waitingReasons.remove(job.getId());
                    selectedWorker.currentJobId = job.getId();
                    record = new TaskExecution(job.getId(), selectedWorker);
                    record.capture(job, eligibleAt);
                    executionTimeline.start(job, selectedWorker, eligibleAt);
                    readySince.remove(job.getId());
                    executionHistory.put(job.getId(), record);
                    runningJobs.put(job.getId(), job);
                    telemetry.dispatched();
                    recordedAt = System.nanoTime();
                    safeRedisSet("job:" + job.getId() + ":status", "RUNNING");
                    safeRedisSet("job:" + job.getId() + ":worker", String.valueOf(selectedWorker.port()));
                    storedAt = System.nanoTime();
                }

//                Worker selectedWorker = availableWorkers.get(ThreadLocalRandom.current().nextInt(availableWorkers.size()));
                System.out.println("[INFO] Dispatching " + job.getId() + " to Worker " + selectedWorker.port());
                try{
                        synchronized (this) {
                            if (job.getStatus() == Job.Status.CANCELLED) continue;
                        }
                        String response = executeJobRequest(job, selectedWorker);
                        synchronized (this) {
                            if (executionHistory.get(job.getId()) != record ||
                                    !runningJobs.containsKey(job.getId())) continue;
                        }
                        telemetry.dispatchPhase(routedAt - cycleStarted, selectedAt - routedAt,
                                recordedAt - selectedAt, storedAt - recordedAt,
                                System.nanoTime() - storedAt);
                        System.out.println("[OK] Job Finished: " + response);

                        if("JOB_ACCEPTED".equals(response)){
                            System.out.println("[ASYNC] Job " + job.getId() + " accepted by worker. Waiting for callback.");
                        }else {
                            System.out.println("[SYNC] Task finished immediately: " + response);
                            synchronized (this) {
                                if (job.getStatus() != Job.Status.CANCELLED) {
                                    runningJobs.remove(job.getId());
                                    completeJob(job, response, record);
                                    selectedWorker.decrementCurrentLoad();
                                    signalCapacityAvailable();
                                }
                            }
                        }

                } catch (Exception e){
                    synchronized (this) {
                        if (executionHistory.get(job.getId()) != record ||
                                !runningJobs.containsKey(job.getId())) continue;
                    }
                    if (e.getMessage() != null && e.getMessage().contains("ERROR_WORKER_SATURATED")) {
                        synchronized (this) {
                            if (job.getStatus() == Job.Status.CANCELLED) continue;
                            // A capacity rejection means the worker never ran the job. It
                            // must not consume a job retry or briefly publish FAILED.
                            record.requeue("Worker at capacity");
                            executionTimeline.discard(job.getId());
                            telemetry.event("DISPATCH_REJECTED", selectedWorker.host() + ":" + selectedWorker.port());
                            runningJobs.remove(job.getId());
                            executionHistory.remove(job.getId(), record);
                            selectedWorker.decrementCurrentLoad();
                            if (selectedWorker.getCurrentLoad() == 0) selectedWorker.currentJobId = null;
                            job.setStatus(Job.Status.PENDING);
                            readySince.putIfAbsent(job.getId(), eligibleAt);
                            safeRedisSet("job:" + job.getId() + ":status", "PENDING");
                            waitingReasons.put(job.getId(), "Worker at capacity; waiting for a slot");
                            taskQueue.offer(job);
                        }
                        awaitCapacityChange();
                        continue;
                    }
                    synchronized (this) {
                        if (job.getStatus() == Job.Status.CANCELLED) continue;
                    System.err.println("[FAIL] Job " + job.getId() + " Error: " + e.getMessage());
                    record.fail(e.getMessage());
                    telemetry.failed();
                    executionTimeline.finish(job.getId(), "FAILED", e.getMessage());

                    runningJobs.remove(job.getId());

                    if (selectedWorker != null) {
                        selectedWorker.currentJobId = null;
                        selectedWorker.decrementCurrentLoad();
                        signalCapacityAvailable();
                    }

                    // We fail the job here and not give it retry if its a deployment issue
                    if (e.getMessage().contains("ALREADY in use") || e.getMessage().contains("Deployment Rejected")) {
                        System.err.println("[FAIL-FAST] Non-recoverable error. Cancelling retries.");
                        job.setStatus(Job.Status.FAILED);
                        safeRedisSet("job:" + job.getId() + ":status", "FAILED");
                        safeRedisSrem("system:active_jobs", job.getId());
                        cancelChildren(job.getId());
                        // We do NOT call handleJobFailure(job) here, so it won't retry/become DEAD.
                    } else{
                        handleJobFailure(job);
//                    history.put(job.getId(), job.getStatus());
                    }
                    recordWorkerHistory(job, record);
                    }
                }
            }
        }

    /**
 * Handles the failure of a job. Increments the job's retry count.
 * If the retry count exceeds a threshold (3 retries), the job is marked as DEAD, moved to the dead-letter queue,
 * and any dependent child jobs are cancelled. Otherwise, the job is re-queued for another attempt.
 * Special handling is included for auto-scale jobs to prevent infinite retries on deployment issues.
 *
 * @param job The {@link Job} that failed.
 */
    private void handleJobFailure(Job job) {
        // If the worker is autoscaled messed up then immediately fail it and not send it to the queue for retry
        if (job.getId().startsWith("WRK-")) {
            System.err.println("[SCALER] Auto-scale job " + job.getId() + " failed. Abandoning job so Scaler can pick a new port.");
            job.setStatus(Job.Status.FAILED);
            safeRedisSet("job:" + job.getId() + ":status", "FAILED");
            safeRedisSrem("system:active_jobs", job.getId());
            return;
        }

        TaskExecution record = executionHistory.get(job.getId());
        job.incrementRetry();
        if(job.getRetryCount() > 3) {
            job.setStatus(Job.Status.DEAD);

            safeRedisSet("job:" + job.getId() + ":status", "DEAD");
            //  Remove from active set since it is dead
            safeRedisSrem("system:active_jobs", job.getId());

            if (record != null) record.status = Job.Status.DEAD;
//            history.put(job.getId(), Job.Status.DEAD);
            System.err.println("Job Moved to DLQ (Max Retries): " + job);
            this.deadLetterQueue.offer(job);
            cancelChildren(job.getId());

        } else{
            job.setStatus(Job.Status.FAILED);
            System.err.println("Job Failed. Retrying... (" + job.getRetryCount() + "/3)");
            job.setStatus(Job.Status.PENDING);

            safeRedisSet("job:" + job.getId() + ":status", "PENDING");
            // We leave the record as FAILED for now so history shows it failed
            //history.put(job.getId(), Job.Status.PENDING);
            taskQueue.offer(job);
            readySince.put(job.getId(), System.currentTimeMillis());
        }
    }

    /**
 * Processes callbacks from workers regarding the completion or failure of asynchronous jobs.
 * The payload typically contains the job ID, status (COMPLETED/FAILED), and an optional result message.
 * This method updates the job's status, clears the worker's active job flag, and triggers completion or failure handling.
 *
 * @param payload The callback string from the worker (e.g., "JOB-123|COMPLETED|Result: 5050").
 */
    public synchronized void handleJobCallback(String payload){
        // New workers identify their port so an old attempt cannot complete a retry on another worker.
        String[] parts = payload.split("\\|", 4);
        if (parts.length < 2) return;

        String jobId = parts[0];
        String statusStr = parts[1];
        int callbackPort = -1;
        String result = (parts.length > 2) ? parts[2] : "";
        if (parts.length > 3 && parts[2].startsWith("worker=")) {
            try { callbackPort = Integer.parseInt(parts[2].substring(7)); }
            catch (NumberFormatException ignored) { return; }
            result = parts[3];
        } else if (parts.length > 3) result = parts[2] + "|" + parts[3];

        TaskExecution record = executionHistory.get(jobId);
        if (callbackPort != -1 && record != null && record.assignedWorker != null
                && record.assignedWorker.port() != callbackPort) {
            System.err.println("[WARN] Ignoring callback from previous worker for " + jobId);
            return;
        }
        Job job = runningJobs.remove(jobId);
        if (job != null && job.getStatus() == Job.Status.CANCELLED) return;

        if(record != null && job != null){
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
                        signalCapacityAvailable();
                    }
//                    if(job != null) job.setStatus(Job.Status.COMPLETED);
//                }
            } else {
                System.err.println("[ASYNC] [FAILED] Callback: Job " + jobId + " Failed.");
                record.fail(result);
                telemetry.failed();
                executionTimeline.finish(jobId, "FAILED", result);
                handleJobFailure(job);
                recordWorkerHistory(job, record);
                if (record.assignedWorker != null) {
                    record.assignedWorker.decrementCurrentLoad();
                    signalCapacityAvailable();
                }
            }
        } else {
            System.err.println("[WARN] Received callback for unknown or inactive Job ID: " + jobId);
        }


    }

    /**
 * Marks a job as completed. Updates the job's status in Redis, removes it from the active jobs set,
 * and records its completion in the execution history. It also decrements the load on the assigned worker,
 * updates worker completion statistics and history, propagates affinity to child jobs, and unlocks any dependent jobs.
 *
 * @param job The {@link Job} that completed.
 * @param result The result string returned by the worker.
 * @param record The {@link TaskExecution} record associated with this job's execution.
 */
    private void completeJob(Job job, String result, TaskExecution record){
        record.complete(result);
        telemetry.completed();
        executionTimeline.finish(job.getId(), "COMPLETED", "");

        if (job != null) {
            job.setStatus(Job.Status.COMPLETED);
            safeRedisSet("job:" + job.getId() + ":status", "COMPLETED");
            safeRedisSet("job:" + job.getId() + ":result", result);

            // Remove from active set as it is completed.
            safeRedisSrem("system:active_jobs", job.getId());
        }

        if (record.assignedWorker != null) {
            String wKey = String.valueOf(record.assignedWorker.port());
            workerCompletionStats.merge(wKey, 1, Integer::sum);

            recordWorkerHistory(job, record);

            propagateAffinity(job.getId(), wKey);
        }
        unlockChildren(job.getId());
    }

    private void recordWorkerHistory(Job job, TaskExecution record) {
        if (job == null || record == null || record.assignedWorker == null) return;
        String workerKey = String.valueOf(record.assignedWorker.port());
        java.util.Deque<Job> history = workerRecentHistory.computeIfAbsent(workerKey,
                key -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        history.removeIf(previous -> previous.getId().equals(job.getId()));
        history.addLast(job);
        while (history.size() > 10) history.removeFirst();
    }

    /**
 * Selects the most suitable worker for a given job from a list of available workers.
 * Prioritizes workers with job affinity (sticky scheduling) if specified by the job.
 * Otherwise, it selects the least loaded worker that is not saturated.
 *
 * @param job The {@link Job} to be dispatched.
 * @param availableWorkers A list of {@link Worker} instances capable of executing the job.
 * @return The selected {@link Worker}, or {@code null} if no suitable worker is found.
 */
    private Worker selectAndReserveWorker(Job job, List<Worker> availableWorkers){
        if(job.getPreferredWorkerId() != null){
            for(Worker w: availableWorkers){
                if(String.valueOf(w.port()).equals(job.getPreferredWorkerId()) && w.tryReserveSlot()){
                    System.out.println("[AFFINITY] Sticky Scheduling: Routing Job " + job.getId() + " to Worker " + w.port());
                    return w;
                }
            }
        }

        // Sort only the small eligible-worker snapshot. tryReserveSlot performs
        // the capacity check and reservation under the same worker lock.
        List<Worker> candidates = new ArrayList<>(availableWorkers);
        candidates.sort(Comparator.comparingInt(Worker::getCurrentLoad));
        for(Worker worker: candidates){
            if(worker.tryReserveSlot()) return worker;
        }
        return null;
    }

    /**
 * Propagates worker affinity from a completed parent job to its dependent child jobs.
 * If a child job requires affinity and its parent completed on a specific worker, the child job
 * will be 'locked' to that same worker for execution, if that worker is still alive.
 *
 * @param parentId The ID of the completed parent job.
 * @param workerPortId The port ID of the worker where the parent job was executed.
 */
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

    /**
 * Executes a job request on a specified worker based on the job's payload type.
 * This method acts as a dispatcher for different types of job execution (standard, deploy, run one-off, archive).
 * It sets the worker's current job ID before execution.
 *
 * @param job The {@link Job} to execute.
 * @param worker The {@link Worker} on which to execute the job.
 * @return The response string from the worker after executing the job.
 * @throws Exception If an error occurs during job execution or communication with the worker.
 */
    private String executeJobRequest(Job job, Worker worker) throws Exception {
        String rawPayload = job.getPayload();
        String actualPayload = rawPayload;

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

    /**
 * Executes a standard task on a worker. This typically involves sending a simple RUN command
 * with the job ID and the task payload.
 *
 * @param job The {@link Job} to execute.
 * @param worker The {@link Worker} on which to execute the task.
 * @param payload The raw payload for the task.
 * @return The response from the worker.
 * @throws Exception If an error occurs during execution.
 */
    private String executeStandardTask(Job job, Worker worker, String payload) throws Exception {
        // NEW FORMAT: "JOB-123|calc.py"
        String payloadWithId = job.getId() + "|" + payload;
        return sendExecuteCommand(worker, TitanProtocol.OP_RUN, payloadWithId);
    }

    private synchronized ServiceEndpoint reserveService(Job job, Worker worker, int port) {
        if (port < 1 || port > 65535) throw new IllegalArgumentException("Deployment Rejected: Invalid listening port");
        if (liveServiceMap.containsKey(job.getId()) || pendingServiceEndpoints.containsKey(job.getId())
                || serviceEndpoints.containsKey(job.getId()) || stoppingServices.contains(job.getId()))
            throw new IllegalStateException("Deployment Rejected: Service ID already in use");
        ServiceEndpoint endpoint = new ServiceEndpoint(worker.host(), port);
        if (serviceEndpoints.containsValue(endpoint) || pendingServiceEndpoints.containsValue(endpoint))
            throw new IllegalStateException("Deployment Rejected: Port already reserved by another service");
        pendingServiceEndpoints.put(job.getId(), endpoint);
        return endpoint;
    }

    private void watchService(Job job, Worker worker, ServiceEndpoint endpoint, String pid) {
        synchronized (this) {
            if (!pendingServiceEndpoints.containsKey(job.getId())
                    || job.getStatus() == Job.Status.CANCELLED || job.getStatus() == Job.Status.FAILED) return;
            readinessMonitor.watch(job.getId(), endpoint.host(), endpoint.port(), ready -> {
            boolean stopChild = false;
            boolean cancelDependents = false;
            synchronized (this) {
                if (!pendingServiceEndpoints.remove(job.getId(), endpoint)) return;
                Job running = runningJobs.remove(job.getId());
                if (running == null || job.getStatus() == Job.Status.CANCELLED) return;
                TaskExecution record = executionHistory.get(job.getId());
                worker.currentJobId = null;
                worker.decrementCurrentLoad();
                signalCapacityAvailable();
                if (ready) {
                    liveServiceMap.put(job.getId(), worker);
                    serviceEndpoints.put(job.getId(), endpoint);
                    completeJob(job, "DEPLOYED_SUCCESS PID:" + pid + " PORT:" + endpoint.port(), record);
                } else {
                    String reason = "Service did not listen on " + endpoint.host() + ":" + endpoint.port();
                    record.fail(reason);
                    telemetry.failed();
                    executionTimeline.finish(job.getId(), "FAILED", reason);
                    job.setStatus(Job.Status.FAILED);
                    safeRedisSet("job:" + job.getId() + ":status", "FAILED");
                    safeRedisSrem("system:active_jobs", job.getId());
                    recordWorkerHistory(job, record);
                    stoppingServices.add(job.getId());
                    stopChild = true;
                    cancelDependents = true;
                }
            }
            if (stopChild) {
                try { sendExecuteCommand(worker, TitanProtocol.OP_STOP, job.getId()); }
                catch (Exception error) { System.err.println("[DEPLOY] Could not stop unready service: " + error); }
            }
            if (cancelDependents) cancelChildren(job.getId());
            });
        }
    }

    /**
 * Executes a deployment sequence on a worker. This involves staging a file (e.g., a JAR or script)
 * and then starting it as a service on a specified port. It includes checks for port availability
 * and starts asynchronous readiness checks for the declared listening port.
 * Special handling is included for internal auto-scaling deployments.
 *
 * @param job The {@link Job} representing the deployment.
 * @param worker The {@link Worker} on which to deploy.
 * @param payload The deployment payload, including filename, base64 content, and optional target port.
 * @return {@code JOB_ACCEPTED} while readiness checks continue outside the dispatch loop.
 * @throws Exception If validation, staging, or process startup fails.
 */
    private String executeDeploySequence(Job job, Worker worker, String payload) throws Exception {
        boolean started = false;
        try {
            String[] parts = payload.split("\\|", -1);
            if (parts.length < 3 || parts[1].isBlank())
                throw new IllegalArgumentException("Deployment Rejected: Invalid deployment payload");

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

            String portString = (parts.length > 3) ? parts[3] : "";
            if (portString.isBlank() && filename.equalsIgnoreCase("Worker.jar")) portString = "8085";
            if (portString.isBlank())
                throw new IllegalArgumentException("Deployment Rejected: A listening port is required");
            int targetPort;
            try { targetPort = Integer.parseInt(portString); }
            catch (NumberFormatException error) {
                throw new IllegalArgumentException("Deployment Rejected: Invalid listening port", error);
            }
            if (targetPort < 1 || targetPort > 65535)
                throw new IllegalArgumentException("Deployment Rejected: Invalid listening port");
            if (isWorkerAlive(worker.host(), targetPort))
                throw new IllegalStateException("Deployment Rejected: Port " + targetPort + " is ALREADY in use");
            ServiceEndpoint endpoint = reserveService(job, worker, targetPort);

            // Step 1: Stage
            String stagePayload = filename + "|" + base64Script;
            String stageResp = sendExecuteCommand(worker, TitanProtocol.OP_STAGE, stagePayload);
            if (!stageResp.contains("FILE_SAVED")) {
                throw new RuntimeException("Staging failed. Expected FILE_SAVED, got: " + stageResp);
            }
            System.out.println("[OK] File Staged");

            // Step 2: Start
            String startPayload = filename + "|" + job.getId() + "|" + targetPort;

            String startResp = sendExecuteCommand(worker, TitanProtocol.OP_START_SERVICE, startPayload);
            if (!startResp.contains("DEPLOYED_SUCCESS")) {
                throw new IllegalStateException("Deployment Rejected: " + startResp);
            }
            started = true;

            String pid = startResp.contains("PID:") ? startResp.split("PID:")[1].trim() : "UNKNOWN";
            watchService(job, worker, endpoint, pid);
            return "JOB_ACCEPTED";
        } catch (Exception e) {
            pendingServiceEndpoints.remove(job.getId());
            readinessMonitor.cancel(job.getId());
            if (started) {
                stoppingServices.add(job.getId());
                try { sendExecuteCommand(worker, TitanProtocol.OP_STOP, job.getId()); }
                catch (Exception stopError) { System.err.println("[DEPLOY] Cleanup failed: " + stopError); }
            }
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

    /**
 * Checks if a worker is alive and reachable on a given host and port by attempting to open a socket connection.
 *
 * @param host The hostname or IP address of the worker.
 *   @param port The port number of the worker.
 * @return {@code true} if the worker is reachable, {@code false} otherwise.
 */
    private boolean isWorkerAlive(String host, int port) {
        try (Socket s = new Socket()) {
            s.connect(new java.net.InetSocketAddress(host, port), 300);
            return true;
        } catch (IOException e) {
            return false;
        }
    }


    /**
 * Executes a one-off script or program on a worker. This involves staging the script file
 * (potentially with arguments) and then instructing the worker to run it asynchronously.
 *
 * @param job The {@link Job} representing the one-off execution.
 * @param worker The {@link Worker} on which to run the script.
 * @param payload The payload containing filename, optional arguments, and base64 script content.
 * @return The response from the worker, typically indicating job acceptance.
 * @throws Exception If staging fails or the run command fails.
 */
    private String executeRunOneOff(Job job, Worker worker, String payload) throws Exception {
        String[] parts = payload.split("\\|");

        if(parts.length < 3) throw new RuntimeException("Invalid Run Payload");

        String filename = parts[1];
        String args = "";
        String base64Script = "";

//        if (parts[2].length() > 200) {
//            base64Script = parts[2];
//            args = "";
//        }
        if (parts.length >= 4) {
            // Case 1 --> We have 4+ parts.
            // Structure must be: HEADER | FILENAME | ARGS | CODE | [REQ]
            args = parts[2];
            base64Script = parts[3];
        }
        else {
            // Case 2: We have exactly 3 parts.
            // Structure must be: HEADER | FILENAME | CODE
            // (Arguments are implied to be empty)
            // Fallback for short scripts without args
            args = "";
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

    /**
 * Executes a job from an archived (ZIP) asset on a worker.
 * This method resolves the archive pointer to get the entry point and base64 content of the archive,
 * then sends it to the worker for execution.
 *
 * @param job The {@link Job} to execute from an archive.
 * @param worker The {@link Worker} on which to run the archive job.
 * @param payload The payload containing the archive pointer (e.g., "zip_name.zip/entry.py").
 * @return The response from the worker.
 * @throws Exception If resolving the archive pointer fails or the execution command fails.
 */
    private String executeRunArchive(Job job, Worker worker, String payload) throws Exception {
        String [] parts = payload.split("\\|");

        String pointer = parts[1];
//        String args = (parts.length > 2) ? parts[2] : "";

        AssetManager.ArchiveInfo fileInfo = AssetManager.resolvePointer(pointer);

        String workerPayload = job.getId() + "|" + fileInfo.entryPoint + "|" + fileInfo.base64Content;

        System.out.println("[ARCHIVE] Dispatching Archive Job " + job.getId() + " (Zip: " + fileInfo.zipName + ")");

        return sendExecuteCommand(worker, TitanProtocol.OP_RUN_ARCHIVE, workerPayload);

    }

    /**
 * Starts a long-running service from an archived (ZIP) asset on a worker.
 * This method resolves the archive pointer, extracts the entry point and base64 content of the archive,
 * and instructs the worker to start it as a managed service on a specified port.
 *
 * @param job The {@link Job} representing the archived service.
 * @param worker The {@link Worker} on which to start the service.
 * @param payload The payload containing the archive pointer, optional arguments, and target port.
 * @return {@code JOB_ACCEPTED} while readiness checks continue.
 * @throws Exception If resolving the archive pointer fails or the service start command fails.
 */
    private String executeServiceArchive(Job job, Worker worker, String payload) throws Exception {
        String[] parts = payload.split("\\|", -1);
        if (parts.length < 4 || parts[3].isBlank())
            throw new IllegalArgumentException("Deployment Rejected: A listening port is required");
        int port;
        try { port = Integer.parseInt(parts[3]); }
        catch (NumberFormatException error) {
            throw new IllegalArgumentException("Deployment Rejected: Invalid listening port", error);
        }
        if (port < 1 || port > 65535)
            throw new IllegalArgumentException("Deployment Rejected: Invalid listening port");
        if (isWorkerAlive(worker.host(), port))
            throw new IllegalStateException("Deployment Rejected: Port " + port + " is ALREADY in use");
        ServiceEndpoint endpoint = reserveService(job, worker, port);
        boolean started = false;
        try {
            AssetManager.ArchiveInfo fileInfo = AssetManager.resolvePointer(parts[1]);
            String workerPayload = job.getId() + "|" + fileInfo.entryPoint + "|" + port + "|" + fileInfo.base64Content;
            String response = sendExecuteCommand(worker, TitanProtocol.OP_START_SERVICE_ARCHIVE, workerPayload);
            if (!response.contains("DEPLOYED_SUCCESS"))
                throw new IllegalStateException("Deployment Rejected: " + response);
            started = true;
            String pid = response.contains("PID:") ? response.split("PID:")[1].trim() : "UNKNOWN";
            watchService(job, worker, endpoint, pid);
            return "JOB_ACCEPTED";
        } catch (Exception error) {
            pendingServiceEndpoints.remove(job.getId(), endpoint);
            readinessMonitor.cancel(job.getId());
            if (started) {
                stoppingServices.add(job.getId());
                try { sendExecuteCommand(worker, TitanProtocol.OP_STOP, job.getId()); }
                catch (Exception stopError) { System.err.println("[DEPLOY] Cleanup failed: " + stopError); }
            }
            throw error;
        }
    }

    /**
 * Sends an RPC command to a specific worker and handles the response.
 * This is a utility method for communicating with workers.
 *
 * @param worker The {@link Worker} to send the command to.
 * @param opCode The operation code ({@link TitanProtocol} constant) for the command.
 * @param payload The payload string for the command.
 * @return The response string from the worker.
 * @throws Exception If the response indicates an error or communication fails.
 */
    private String sendExecuteCommand(Worker worker, byte opCode, String payload) throws Exception {
        String response = schedulerClient.sendRequest(worker.host(), worker.port(), opCode, payload);
        if (response == null || response.startsWith("ERROR") || response.startsWith("JOB_FAILED")) {
            System.err.println("[FAIL] Job Failed on Worker " + worker.port() + ": " + response);
            throw new RuntimeException("Worker Error: " + response);
        }
        return response;
    }

    /**
 * Stops a remote service identified by its service ID.
 * It looks up the worker hosting the service and sends a STOP command to that worker.
 * If successful, the service is removed from the live service map.
 *
 * @param serviceId The ID of the service to stop.
 * @return A success or error message indicating the outcome of the stop operation.
 */
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
            // Keep the ID reserved until the worker's exit callback unregisters it.
            // STOPPED means a kill was requested, not that the process has exited.
            return response;
        } catch (Exception e) {
            return "COMMUNICATION_ERROR: " + e.getMessage();
        }
    }

    /**
 * Initiates a graceful shutdown of a specific worker node.
 * It first stops any services hosted by the target worker, then sends a KILL_WORKER command to the worker.
 * Permanent workers cannot be shut down via this method.
 *
 * @param targetHost The hostname or IP address of the worker to shut down.
 * @param targetPort The port number of the worker to shut down.
 * @return A success or error message indicating the outcome of the shutdown operation.
 */
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

        // Autoscaled workers are child services on their parent worker. Stop
        // that service first so its process supervisor sees an intentional
        // shutdown and does not relaunch the child after OP_KILL_WORKER.
        String autoWorkerPrefix = "WRK-" + targetPort + "-";
        for (String serviceId : new java.util.ArrayList<>(liveServiceMap.keySet())) {
            if (serviceId.startsWith(autoWorkerPrefix)) {
                String result = stopRemoteService(serviceId);
                System.out.println("[INFO] Stopping autoscaled worker service " + serviceId + ": " + result);
            }
        }

        for(Map.Entry<String, Worker> entry: liveServiceMap.entrySet()){
            if(entry.getValue().equals(targetWorker)){
                servicesToStop.add(entry.getKey());
            }
        }

        for (String serviceId : servicesToStop) {
            System.out.println("[INFO] Stopping child service: " + serviceId);
            stopRemoteService(serviceId);
        }

        liveServiceMap.entrySet().removeIf(entry -> {
            boolean match = entry.getKey().startsWith(autoWorkerPrefix);
            if (match) serviceEndpoints.remove(entry.getKey());
            return match;
        });
        // Send the Kill Command to the Worker
        try {
            schedulerClient.sendRequest(targetWorker.host(), targetWorker.port(), TitanProtocol.OP_KILL_WORKER, "NOW");
        } catch (Exception e) {
            System.err.println("[WARN] Worker might have died before receiving ACK: " + e.getMessage());
        }
        workerRegistry.getWorkerMap().remove(targetWorker.host() + ":" + targetWorker.port());
        schedulerClient.evict(targetWorker.host(), targetWorker.port());
        return "SUCCESS: Worker " + targetPort + " and " + servicesToStop.size() + " services shut down.";
    }

    /**
 * Unlocks child jobs that were dependent on a newly completed parent job.
 * It iterates through jobs in the DAG waiting room, resolves the dependency for the given parent ID,
 * and if all dependencies for a child job are met, it moves that child job to the active task queue.
 *
 * @param parentId The ID of the parent job that has just completed.
 */
    private synchronized void unlockChildren(String parentId){
        for(Job waitingJob: dagWaitingRoom.values()){
            if(waitingJob.getDependenciesIds()!=null && waitingJob.getDependenciesIds().contains(parentId)){
                waitingJob.resolveDependencies(parentId);

                if(waitingJob.isReady()){
                    System.out.println("[INFO] DAG: All dependencies met for " + waitingJob.getId() + ". Moving to Active Queue.");
                    if (dagWaitingRoom.remove(waitingJob.getId(), waitingJob)) {
                        enqueueReadyJob(waitingJob);
                    }
                }
            }
        }
    }

    /**
 * Recursively cancels child jobs whose parent job has failed.
 * When a parent job fails, all its direct and indirect dependent jobs are marked as DEAD
 * and moved to the dead-letter queue.
 *
 * @param failedParentId The ID of the parent job that failed.
 */
    public void cancelChildren(String failedParentId){
        for (Job job : new ArrayList<>(knownJobs.values())) {
            if (job.getDependenciesIds().contains(failedParentId)) cancelJob(job.getId());
        }
    }

    private void signalCapacityAvailable() {
        capacitySignal.offer(Boolean.TRUE);
    }

    private void awaitCapacityChange() throws InterruptedException {
        // The token is retained if it arrives before the dispatcher waits. A timeout
        // still retries when a worker disappears without delivering a signal.
        capacitySignal.poll(1, TimeUnit.SECONDS);
    }

    private void sampleTelemetry() {
        try {
            long now = System.currentTimeMillis();
            long oldestReady = taskQueue.stream()
                    .mapToLong(job -> now - readySince.getOrDefault(job.getId(), now)).max().orElse(0);
            long oldestParked = capabilityWaiting.values().stream()
                    .mapToLong(job -> now - readySince.getOrDefault(job.getId(), now)).max().orElse(0);
            telemetry.sample(taskQueue.size(), dagWaitingRoom.size(), capabilityWaiting.size(),
                    waitingRoom.size(), oldestReady, oldestParked,
                    runningJobs.size(), workerRegistry.getWorkers(),
                    redis.isConnected(), executionTimeline.droppedWrites(), executionTimeline.pendingWrites());
        } catch (RuntimeException error) {
            System.err.println("[METRICS] Sample failed: " + error.getMessage());
        }
    }

    public String timelineJSON(long since, int limit) {
        return executionTimeline.json(since, Math.min(limit, 5000));
    }
    public String spanHistoryJSON(String day, int limit) {
        try { return executionTimeline.history(java.time.LocalDate.parse(day), limit); }
        catch (java.time.format.DateTimeParseException error) { return "ERROR: Invalid UTC date"; }
    }
    public String metricsJSON(long since, int limit) { return telemetry.json(since, limit); }

    public String bulkStatusJSON(String ids) {
        StringBuilder json = new StringBuilder("{");
        int count = 0;
        for (String raw : ids.split(",")) {
            if (count >= 500) break;
            String id = raw.trim();
            if (id.isEmpty()) continue;
            if (count++ > 0) json.append(',');
            Job job = knownJobs.get(id);
            TaskExecution record = executionHistory.get(id);
            json.append(ExecutionTimeline.quoted(id)).append(":{\"status\":")
                    .append(ExecutionTimeline.quoted(job == null ? "UNKNOWN" : job.getStatus().name()))
                    .append(",\"waiting_reason\":")
                    .append(ExecutionTimeline.quoted(waitingReasons.getOrDefault(id, "")))
                    .append(",\"worker\":")
                    .append(ExecutionTimeline.quoted(record == null || record.assignedWorker == null
                            ? "" : record.assignedWorker.host() + ":" + record.assignedWorker.port()))
                    .append(",\"eligible_at\":").append(record == null ? readySince.getOrDefault(id, 0L) : record.eligibleAt)
                    .append(",\"started_at\":").append(record == null ? 0 : record.startTime)
                    .append(",\"ended_at\":").append(record == null ? 0 : record.endTime)
                    .append(",\"attempt\":").append(record == null ? job == null ? 0 : job.getRetryCount() + 1 : record.attempt)
                    .append(",\"priority\":").append(record == null ? job == null ? 0 : job.getPriority() : record.priority)
                    .append(",\"failure_reason\":")
                    .append(ExecutionTimeline.quoted(record == null ? "" : record.failureReason))
                    .append(",\"result\":")
                    .append(ExecutionTimeline.quoted(record == null || record.output == null ? "" : record.output))
                    .append(",\"parents\":[");
            List<String> parents = record == null ? job == null ? List.of() : job.getDependenciesIds() : record.declaredParents;
            for (int i = 0; i < parents.size(); i++) {
                if (i > 0) json.append(',');
                json.append(ExecutionTimeline.quoted(parents.get(i)));
            }
            json.append(']')
                    .append('}');
        }
        return json.append('}').toString();
    }

    public String queueBoardJSON(int limit) {
        int bounded = Math.max(1, Math.min(limit, 500));
        StringBuilder json = new StringBuilder("{\"counts\":{\"ready\":").append(taskQueue.size())
                .append(",\"dependencies\":").append(dagWaitingRoom.size())
                .append(",\"capability\":").append(capabilityWaiting.size())
                .append(",\"delayed\":").append(waitingRoom.size())
                .append(",\"running\":").append(runningJobs.size())
                .append("},\"jobs\":[");
        int count = 0;
        List<Job> boardJobs = new ArrayList<>(knownJobs.values());
        boardJobs.sort(Comparator.comparingInt(Job::getPriority).reversed()
                .thenComparing(job -> readySince.getOrDefault(job.getId(), Long.MAX_VALUE)));
        for (Job job : boardJobs) {
            if (count >= bounded) break;
            if (job.getStatus() != Job.Status.PENDING && job.getStatus() != Job.Status.RUNNING) continue;
            if (count++ > 0) json.append(',');
            String reason = waitingReasons.get(job.getId());
            if (reason == null) {
                if (dagWaitingRoom.containsKey(job.getId())) reason = "Waiting for parents";
                else if (waitingRoom.stream().anyMatch(delayed -> delayed.getJob() == job)) reason = "Scheduled for later";
                else if (runningJobs.containsKey(job.getId())) reason = "Executing";
                else reason = "Ready for dispatch";
            }
            String lane = dagWaitingRoom.containsKey(job.getId()) ? "blocked"
                    : capabilityWaiting.containsKey(job.getId()) ? "parked"
                    : waitingRoom.stream().anyMatch(delayed -> delayed.getJob() == job) ? "delayed"
                    : runningJobs.containsKey(job.getId()) ? "running" : "ready";
            json.append("{\"job_id\":").append(ExecutionTimeline.quoted(job.getId()))
                    .append(",\"status\":").append(ExecutionTimeline.quoted(job.getStatus().name()))
                    .append(",\"lane\":").append(ExecutionTimeline.quoted(lane))
                    .append(",\"reason\":").append(ExecutionTimeline.quoted(reason))
                    .append(",\"capability\":").append(ExecutionTimeline.quoted(extractSkillRequirement(job)))
                    .append(",\"priority\":").append(job.getPriority())
                    .append(",\"eligible_at\":").append(readySince.getOrDefault(job.getId(), 0L))
                    .append('}');
        }
        json.append("],\"dead_letter\":[");
        count = 0;
        for (Job job : deadLetterQueue) {
            if (count++ >= bounded) break;
            if (count > 1) json.append(',');
            TaskExecution record = executionHistory.get(job.getId());
            json.append("{\"job_id\":").append(ExecutionTimeline.quoted(job.getId()))
                    .append(",\"reason\":").append(ExecutionTimeline.quoted(
                            record == null ? "" : record.failureReason)).append('}');
        }
        json.append("],\"services\":[");
        count = 0;
        for (Map.Entry<String, ServiceEndpoint> entry : serviceEndpoints.entrySet()) {
            if (count++ >= bounded) break;
            if (count > 1) json.append(',');
            Worker worker = liveServiceMap.get(entry.getKey());
            TaskExecution record = executionHistory.get(entry.getKey());
            json.append("{\"id\":").append(ExecutionTimeline.quoted(entry.getKey()))
                    .append(",\"host\":").append(ExecutionTimeline.quoted(entry.getValue().host()))
                    .append(",\"port\":").append(entry.getValue().port())
                    .append(",\"worker\":").append(ExecutionTimeline.quoted(worker == null ? "" : worker.host() + ":" + worker.port()))
                    .append(",\"since\":").append(record == null ? 0 : record.endTime)
                    .append(",\"deploy_attempts\":").append(record == null ? 0 : record.attempt)
                    .append('}');
        }
        return json.append("]}").toString();
    }

    /** Cancel a job and every transitive dependent. A terminal job is never rewritten. */
    public String cancelJob(String jobId) {
        if (jobId == null || jobId.isBlank()) return "ERROR: Missing job ID";
        List<Map.Entry<Worker, String>> toKill = new ArrayList<>();
        List<Map.Entry<Worker, String>> servicesToStop = new ArrayList<>();
        int cancelled = 0;
        synchronized (this) {
            Job root = knownJobs.get(jobId);
            if (root == null) return "ERROR: Unknown job ID";
            if (isTerminal(root.getStatus())) return "ALREADY_TERMINAL:" + root.getStatus();
            Queue<String> pending = new ArrayDeque<>();
            Set<String> visited = new HashSet<>();
            pending.add(jobId);
            while (!pending.isEmpty()) {
                String id = pending.remove();
                if (!visited.add(id)) continue;
                for (Job child : knownJobs.values()) {
                    if (child.getDependenciesIds().contains(id)) pending.add(child.getId());
                }
                Job job = knownJobs.get(id);
                if (job == null || isTerminal(job.getStatus())) continue;
                job.setStatus(Job.Status.CANCELLED);
                taskQueue.remove(job);
                capabilityWaiting.remove(id);
                waitingReasons.remove(id);
                readySince.remove(id);
                waitingRoom.removeIf(delayed -> delayed.getJob() == job);
                dagWaitingRoom.remove(id);
                TaskExecution record = executionHistory.computeIfAbsent(id, key -> new TaskExecution(id, null));
                record.status = Job.Status.CANCELLED;
                record.endTime = System.currentTimeMillis();
                record.output = id.equals(jobId) ? "Cancelled by request" : "Cancelled with parent " + jobId;
                executionTimeline.finish(id, "CANCELLED", record.output);
                Job running = runningJobs.remove(id);
                boolean pendingService = pendingServiceEndpoints.remove(id) != null;
                if (pendingService) {
                    readinessMonitor.cancel(id);
                    stoppingServices.add(id);
                }
                if (running != null && record.assignedWorker != null) {
                    Worker worker = record.assignedWorker;
                    worker.currentJobId = null;
                    worker.decrementCurrentLoad();
                    signalCapacityAvailable();
                    recordWorkerHistory(job, record);
                    if (pendingService) servicesToStop.add(Map.entry(worker, id));
                    else toKill.add(Map.entry(worker, id));
                }
                safeRedisSet("job:" + id + ":status", "CANCELLED");
                safeRedisSrem("system:active_jobs", id);
                cancelled++;
            }
        }
        for (Map.Entry<Worker, String> target : toKill) {
            try {
                sendExecuteCommand(target.getKey(), TitanProtocol.OP_CANCEL_JOB, target.getValue());
            } catch (Exception error) {
                System.err.println("[CANCEL] Worker kill failed for " + target.getValue() + ": " + error.getMessage());
            }
        }
        for (Map.Entry<Worker, String> target : servicesToStop) {
            try { sendExecuteCommand(target.getKey(), TitanProtocol.OP_STOP, target.getValue()); }
            catch (Exception error) {
                System.err.println("[CANCEL] Service stop failed for " + target.getValue() + ": " + error);
            }
        }
        return "CANCELLED:" + cancelled;
    }

    private static boolean isTerminal(Job.Status status) {
        return status == Job.Status.COMPLETED || status == Job.Status.FAILED
                || status == Job.Status.DEAD || status == Job.Status.CANCELLED;
    }

    // Methods for sending the logs to stream to the UI
    /**
 * Streams a log line for a specific job. The log line is added to an in-memory buffer
 * for real-time retrieval and also appended to a persistent log file on disk.
 * The in-memory buffer maintains a maximum number of lines to prevent excessive memory usage.
 *
 * @param jobId The ID of the job to which the log line belongs.
 * @param line The log line to stream.
 */
    public void logStream(String jobId, String line) {
        if (!safeJobId(jobId)) throw new IllegalArgumentException("Invalid job ID");
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

    /**
 * Appends a log line to a job-specific log file on disk.
 * Log files are stored in the 'titan_server_logs' directory.
 *
 * @param jobId The ID of the job.
 * @param line The log line to append.
 */
    private void appendLogToDisk(String jobId, String line) {
        try {
            java.nio.file.Path directory = java.nio.file.Path.of("titan_server_logs")
                    .toAbsolutePath().normalize();
            java.nio.file.Files.createDirectories(directory);
            java.nio.file.Path logFile = directory.resolve(jobId + ".log").normalize();
            if (!logFile.startsWith(directory) || java.nio.file.Files.isSymbolicLink(logFile))
                throw new IOException("Invalid log file path");
            java.nio.file.Files.writeString(logFile, line + "\n",
                    java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND,
                    java.nio.file.LinkOption.NOFOLLOW_LINKS);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static boolean safeJobId(String id) {
        return id != null && !id.isBlank() && id.length() <= 255
                && !id.equals(".") && !id.equals("..")
                && id.indexOf('/') < 0 && id.indexOf('\\') < 0
                && id.indexOf(':') < 0 && id.indexOf('|') < 0
                && id.chars().noneMatch(c -> c < 32 || c == 127);
    }

    // Helper method for the UI to retrieve the logs
    /**
 * Retrieves a snapshot of the recent log lines for a given job from the in-memory buffer.
 * This method returns a copy of the log list to prevent {@link ConcurrentModificationException}s.
 *
 * @param jobId The ID of the job whose logs are to be retrieved.
 * @return A {@link List} of strings, each representing a log line for the specified job.
 */
//    public List<String> getLogs(String jobId) {
//        return liveLogBuffer.getOrDefault(jobId, new ArrayList<>());
//    }

        /**
 * Retrieves a snapshot of the recent log lines for a given job from the in-memory buffer.
 * This method returns a copy of the log list to prevent {@link ConcurrentModificationException}s.
 *
 * @param jobId The ID of the job whose logs are to be retrieved.
 * @return A {@link List} of strings, each representing a log line for the specified job.
 */
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

    /**
 * Generates a human-readable string containing various system statistics.
 * This includes the number of active workers, job queue sizes, and detailed status for each worker
 * (load, capabilities, and hosted services).
 *
 * @return A formatted string with system statistics.
 */
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

                            sb.append(String.format("    └── [SVC] Service ID: %s\n", serviceId));
                        });
            }
        } else{
            sb.append("[INFO] No workers currently connected.\n");
        }
        return sb.toString();
    }

    /**
 * Generates a JSON string containing various system statistics.
 * This includes the number of active workers, job queue sizes, and detailed status for each worker
 * (port, capabilities, load, active job, recent history, and hosted services).
 *
 * @return A JSON formatted string with system statistics.
 */
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
                    long completedAt = 0;
                    TaskExecution exec = executionHistory.get(j.getId());
                    if (exec != null) {
                        duration = exec.getDuration() + "ms";
                        completedAt = exec.endTime;
                    }
                    json.append(String.format("{\"id\": \"%s\", \"status\": \"%s\", \"time\": \"%s\", \"completed_at\": %d}",
                            j.getId(), exec == null ? j.getStatus() : exec.status, duration, completedAt));
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

        json.append("],\"dag_jobs\":{");
        boolean firstDagJob = true;
        for (Map.Entry<String, TaskExecution> entry : executionHistory.entrySet()) {
            if (!entry.getKey().startsWith("DAG-")) continue;
            if (!firstDagJob) json.append(",");
            TaskExecution exec = entry.getValue();
            String assigned = exec.assignedWorker == null ? "" :
                    exec.assignedWorker.host() + ":" + exec.assignedWorker.port();
            json.append(String.format("\"%s\":{\"status\":\"%s\",\"completed_at\":%d,\"time\":\"%dms\",\"worker\":\"%s\"}",
                    entry.getKey(), exec.status, exec.endTime, exec.getDuration(), assigned));
            firstDagJob = false;
        }
        json.append("},\"execution_spans\":").append(executionTimeline.json(0, 500)).append("}");
        return json.toString();
    }

    /**
 * Retrieves the current status of a job based on its ID.
 *
 * @param id The ID of the job.
 * @return The {@link Job.Status} of the job, or {@link Job.Status#PENDING} if the job is not found in execution history.
 */
    public Job.Status getJobStatus(String id) {
        Job known = knownJobs.get(id);
        if (known != null) return known.getStatus();
        if (executionHistory.containsKey(id)) {
            return executionHistory.get(id).status;
        }
        return Job.Status.PENDING;
    }

    /**
 * Shuts down the scheduler and all its associated executor services.
 * This method gracefully stops the scheduler server, heartbeat executor, and dispatch executor.
 */
    public void stop(){
        if(isRunning){
            isRunning = false;

            if (schedulerServer != null) schedulerServer.stop();

            serverExecutor.shutdownNow();
            heartBeatExecutor.shutdownNow();
            telemetryExecutor.shutdownNow();
            dispatchExecutor.shutdownNow();
            schedulerClient.close();
            readinessMonitor.close();
            executionTimeline.close();
        }
    }
}

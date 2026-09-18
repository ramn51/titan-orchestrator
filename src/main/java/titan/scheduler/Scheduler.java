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
import java.io.FileWriter;
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
    private final ExecutorService dispatchExecutor;
    private final ExecutorService serverExecutor;
    private volatile boolean isRunning = true;
    private int port;

    private int redisPort = TitanConfig.getInt("titan.redis.port", 6379);
    private String redisHost = TitanConfig.get("titan.redis.host", "localhost");


    // JRedis config
    private final TitanJRedisAdapter redis;

    // This is purely for validation purpose
    private final Map<String, TaskExecution> executionHistory = new ConcurrentHashMap<>();

    /**
     * Append-only log of every dispatch attempt, bounded. {@link #executionHistory} is keyed by job
     * ID so a retry overwrites its predecessor — fine for "current state of job X", useless for a
     * timeline, where three attempts must appear as three spans. This keeps both.
     */
    private final java.util.Deque<TaskExecution> spanLog = new java.util.concurrent.ConcurrentLinkedDeque<>();
    private static final int MAX_SPANS = TitanConfig.getInt("titan.metrics.spans.max", 5000);

    // ---------------------------------------------------------------------
    // Durable span history.
    //
    // The in-memory ring above is the live view and stays the fast path. This writes the same
    // spans to day-partitioned JSONL so history survives a restart.
    //
    // Deliberately NOT TitanStore: its command set is GET/SET/SADD/SMEMBERS/SREM, so a time-range
    // read would be SMEMBERS plus one GET per span — thousands of round trips. Spans are an
    // append-only event stream, which is what a log file is for, and the project already persists
    // this way (the AOF, titan_server_logs).
    // ---------------------------------------------------------------------
    private static final boolean SPAN_PERSIST = !"false".equalsIgnoreCase(
            TitanConfig.get("titan.spans.persist", "true"));
    private static final String SPAN_DIR = TitanConfig.get("titan.spans.dir", "titan_spans");
    private static final int SPAN_RETAIN_DAYS = TitanConfig.getInt("titan.spans.retain.days", 7);

    /** Spans awaiting a disk write. Handing off here keeps file I/O off the dispatch thread. */
    private final java.util.concurrent.ConcurrentLinkedQueue<String> spanWriteQueue =
            new java.util.concurrent.ConcurrentLinkedQueue<>();
    private final java.util.concurrent.atomic.AtomicLong spansPersisted = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong spanWriteErrors = new java.util.concurrent.atomic.AtomicLong();

    /** Serialises one span as a single JSON line. */
    private String spanToJsonLine(TaskExecution e) {
        StringBuilder b = new StringBuilder(256);
        String worker = (e.assignedWorker != null) ? String.valueOf(e.assignedWorker.port()) : "unassigned";
        String host = (e.assignedWorker != null) ? e.assignedWorker.host() : "";
        b.append("{\"id\":\"").append(jsonEscape(e.jobId)).append("\"")
         .append(",\"attempt\":").append(e.attempt)
         .append(",\"priority\":").append(e.priority)
         .append(",\"status\":\"").append(e.status).append("\"")
         .append(",\"enqueued_at\":").append(e.enqueuedAt)
         .append(",\"started_at\":").append(e.startTime)
         .append(",\"ended_at\":").append(e.endTime)
         .append(",\"queue_wait_ms\":").append(e.getQueueWaitMs())
         .append(",\"duration_ms\":").append(e.getDuration())
         .append(",\"worker\":\"").append(jsonEscape(worker)).append("\"")
         .append(",\"host\":\"").append(jsonEscape(host)).append("\"")
         .append(",\"worker_permanent\":").append(e.assignedWorker != null && e.assignedWorker.isPermanent())
         .append(",\"reason\":\"").append(jsonEscape(shortReason(e.output))).append("\"")
         .append(",\"parents\":[");
        for (int i = 0; i < e.parents.size(); i++) {
            b.append("\"").append(jsonEscape(e.parents.get(i))).append("\"");
            if (i < e.parents.size() - 1) b.append(",");
        }
        return b.append("]}").toString();
    }

    /** Queues a finished span for durable write. Cheap and non-blocking. */
    private void persistSpan(TaskExecution e) {
        if (!SPAN_PERSIST || e == null) return;
        spanWriteQueue.add(spanToJsonLine(e));
    }

    /** @return The file spans for {@code epochMs} belong in. */
    private static java.io.File spanFileFor(long epochMs) {
        String day = java.time.Instant.ofEpochMilli(epochMs)
                .atZone(java.time.ZoneId.systemDefault()).toLocalDate().toString();
        return new java.io.File(SPAN_DIR, "spans-" + day + ".jsonl");
    }

    /**
     * Drains the queue to today's file in one open/append/close. Runs on the metrics sampler, so a
     * slow disk delays a background flush rather than a dispatch.
     */
    private void flushSpansToDisk() {
        if (!SPAN_PERSIST || spanWriteQueue.isEmpty()) return;
        java.io.File dir = new java.io.File(SPAN_DIR);
        if (!dir.exists() && !dir.mkdirs()) {
            spanWriteErrors.incrementAndGet();
            return;
        }
        java.io.File target = spanFileFor(System.currentTimeMillis());
        int written = 0;
        try (java.io.BufferedWriter w = new java.io.BufferedWriter(
                new java.io.FileWriter(target, true))) {
            String line;
            while ((line = spanWriteQueue.poll()) != null) {
                w.write(line);
                w.newLine();
                written++;
                if (written >= 5000) break;      // bound one flush; the rest goes next tick
            }
        } catch (java.io.IOException ex) {
            spanWriteErrors.incrementAndGet();
            System.err.println("[WARN] Span persistence failed: " + ex.getMessage());
            return;
        }
        if (written > 0) spansPersisted.addAndGet(written);
    }

    /** Deletes span files older than the retention window. Day partitioning makes this a file delete. */
    private void sweepOldSpanFiles() {
        if (!SPAN_PERSIST || SPAN_RETAIN_DAYS <= 0) return;
        java.io.File dir = new java.io.File(SPAN_DIR);
        java.io.File[] files = dir.listFiles((d, n) -> n.startsWith("spans-") && n.endsWith(".jsonl"));
        if (files == null) return;
        java.time.LocalDate cutoff = java.time.LocalDate.now().minusDays(SPAN_RETAIN_DAYS);
        for (java.io.File f : files) {
            try {
                String day = f.getName().substring("spans-".length(), f.getName().length() - ".jsonl".length());
                if (java.time.LocalDate.parse(day).isBefore(cutoff) && f.delete()) {
                    System.out.println("[RETENTION] Removed expired span file " + f.getName());
                }
            } catch (RuntimeException ignored) {
                // an unparseable name is not ours to delete
            }
        }
    }

    /**
     * Reads persisted spans for a time range, newest first.
     * <p>
     * Only the day files overlapping the range are opened, so cost scales with the range asked for
     * rather than with total history.
     *
     * @param fromMs Inclusive lower bound, epoch millis.
     * @param toMs Inclusive upper bound, epoch millis.
     * @param filter Substring the job ID must contain; empty matches all.
     * @param limit Maximum spans returned.
     * @return JSON with a {@code spans} array, matching the live timeline shape.
     */
    public String getSpanHistoryJSON(long fromMs, long toMs, String filter, int limit) {
        int cap = limit <= 0 ? 1000 : Math.min(limit, 20000);
        // Read up to this many matching lines before sorting; bounds memory on a long retention
        // window without capping the sort at the requested limit.
        final int scanCeiling = Math.min(Math.max(cap * 10, 20000), 400000);
        java.util.List<Object[]> scanned = new java.util.ArrayList<>();
        java.util.List<String> hits = new java.util.ArrayList<>();
        java.io.File dir = new java.io.File(SPAN_DIR);

        if (dir.isDirectory()) {
            java.time.LocalDate d = java.time.Instant.ofEpochMilli(fromMs)
                    .atZone(java.time.ZoneId.systemDefault()).toLocalDate();
            java.time.LocalDate end = java.time.Instant.ofEpochMilli(toMs)
                    .atZone(java.time.ZoneId.systemDefault()).toLocalDate();
            java.util.List<java.io.File> days = new java.util.ArrayList<>();
            while (!d.isAfter(end)) {
                java.io.File f = new java.io.File(dir, "spans-" + d + ".jsonl");
                if (f.isFile()) days.add(f);
                d = d.plusDays(1);
            }
            java.util.Collections.reverse(days);          // newest day first

            outer:
            for (java.io.File f : days) {
                java.util.List<String> lines;
                try {
                    lines = java.nio.file.Files.readAllLines(f.toPath());
                } catch (java.io.IOException ex) {
                    continue;
                }
                // Lines are appended in COMPLETION order, so reverse file order is not
                // descending start time — a long job that started first finishes last and lands
                // at the end of the file. Collect, then sort, so `limit` really does keep the
                // most recent spans instead of whichever happened to finish last.
                for (int i = lines.size() - 1; i >= 0; i--) {
                    String line = lines.get(i);
                    if (line.isEmpty()) continue;
                    if (!filter.isEmpty() && !line.contains(filter)) continue;
                    long st = extractLong(line, "\"started_at\":");
                    if (st < fromMs || st > toMs) continue;
                    scanned.add(new Object[]{st, line});
                    if (scanned.size() >= scanCeiling) break outer;
                }
            }
        }

        scanned.sort((a, b) -> Long.compare((Long) b[0], (Long) a[0]));   // newest start first
        for (Object[] row : scanned) {
            if (hits.size() >= cap) break;
            hits.add((String) row[1]);
        }
        boolean cut = scanned.size() > hits.size();

        StringBuilder j = new StringBuilder();
        j.append("{\"now\":").append(System.currentTimeMillis())
         .append(",\"source\":\"disk\"")
         .append(",\"from\":").append(fromMs).append(",\"to\":").append(toMs)
         .append(",\"count\":").append(hits.size())
         .append(",\"truncated\":").append(cut)
         .append(",\"spans\":[");
        for (int i = 0; i < hits.size(); i++) {
            j.append(hits.get(i));
            if (i < hits.size() - 1) j.append(",");
        }
        return j.append("]}").toString();
    }

    /** Pulls a numeric field out of a span line without a JSON parser. */
    private static long extractLong(String line, String key) {
        int i = line.indexOf(key);
        if (i < 0) return -1;
        int st = i + key.length(), e = st;
        while (e < line.length() && (Character.isDigit(line.charAt(e)) || line.charAt(e) == '-')) e++;
        try {
            return Long.parseLong(line.substring(st, e));
        } catch (NumberFormatException ex) {
            return -1;
        }
    }

    /** @return A summary of what is on disk, for the dashboard. */
    public String getSpanHistoryStatsJSON() {
        java.io.File dir = new java.io.File(SPAN_DIR);
        java.io.File[] files = dir.isDirectory()
                ? dir.listFiles((d, n) -> n.startsWith("spans-") && n.endsWith(".jsonl"))
                : null;
        long bytes = 0;
        int days = 0;
        String oldest = "", newest = "";
        if (files != null) {
            java.util.Arrays.sort(files, java.util.Comparator.comparing(java.io.File::getName));
            days = files.length;
            for (java.io.File f : files) bytes += f.length();
            if (days > 0) {
                oldest = files[0].getName();
                newest = files[days - 1].getName();
            }
        }
        return "{\"enabled\":" + SPAN_PERSIST
                + ",\"dir\":\"" + jsonEscape(SPAN_DIR) + "\""
                + ",\"retain_days\":" + SPAN_RETAIN_DAYS
                + ",\"day_files\":" + days
                + ",\"bytes\":" + bytes
                + ",\"oldest\":\"" + jsonEscape(oldest) + "\""
                + ",\"newest\":\"" + jsonEscape(newest) + "\""
                + ",\"written\":" + spansPersisted.get()
                + ",\"queued\":" + spanWriteQueue.size()
                + ",\"errors\":" + spanWriteErrors.get() + "}";
    }

    /** How coarser tiers collapse a block of fine-grained samples into one. */
    private enum Roll {
        /** Keep the peak. Right for levels — a queue spike must survive being summarised. */
        MAX,
        /** Add them up. Right for counters — ten 1s counts become one 10s count. */
        SUM
    }

    /**
     * A time series kept at three resolutions at once.
     * <p>
     * A single ring forces a choice between detail and span: 600 samples is either ten minutes at
     * one-second resolution or ten hours at one-minute. Keeping three rings and rolling older data
     * into coarser ones gives both — roughly 1,700 samples covers twelve hours here, against 43,200
     * for the same span at flat one-second resolution.
     * <p>
     * Every tier keeps the same tuple shape, so a caller reads any resolution with identical code.
     */
    private static final class Series {
        private static final int ROLL_FACTOR = 10;   // 1s -> 10s -> ~100s

        private final java.util.Deque<long[]> fine = new java.util.ArrayDeque<>();
        private final java.util.Deque<long[]> mid = new java.util.ArrayDeque<>();
        private final java.util.Deque<long[]> coarse = new java.util.ArrayDeque<>();
        private final java.util.List<long[]> fineBuf = new java.util.ArrayList<>();
        private final java.util.List<long[]> midBuf = new java.util.ArrayList<>();
        private final int cap;
        private final Roll roll;

        Series(int cap) { this(cap, Roll.MAX); }
        Series(int cap, Roll roll) { this.cap = cap; this.roll = roll; }

        synchronized void add(long... vals) {
            push(fine, vals);
            fineBuf.add(vals);
            if (fineBuf.size() >= ROLL_FACTOR) {
                long[] rolled = collapse(fineBuf);
                push(mid, rolled);
                midBuf.add(rolled);
                fineBuf.clear();
                if (midBuf.size() >= ROLL_FACTOR) {
                    push(coarse, collapse(midBuf));
                    midBuf.clear();
                }
            }
        }

        private void push(java.util.Deque<long[]> d, long[] v) {
            d.addLast(v);
            while (d.size() > cap) d.removeFirst();
        }

        /** Collapses a block into one sample: timestamp of the last, values per the roll mode. */
        private long[] collapse(java.util.List<long[]> block) {
            long[] first = block.get(0);
            long[] out = new long[first.length];
            out[0] = block.get(block.size() - 1)[0];
            for (int i = 1; i < out.length; i++) {
                long acc = (roll == Roll.SUM) ? 0 : Long.MIN_VALUE;
                for (long[] p : block) {
                    if (i >= p.length) continue;
                    acc = (roll == Roll.SUM) ? acc + p[i] : Math.max(acc, p[i]);
                }
                out[i] = (acc == Long.MIN_VALUE) ? 0 : acc;
            }
            return out;
        }

        /**
         * @param res "fine" (1s), "mid" (~10s) or "coarse" (~100s). Unknown values fall back to fine.
         */
        synchronized java.util.List<long[]> snapshot(String res) {
            if ("coarse".equals(res)) return new java.util.ArrayList<>(coarse);
            if ("mid".equals(res)) return new java.util.ArrayList<>(mid);
            return new java.util.ArrayList<>(fine);
        }

        synchronized java.util.List<long[]> snapshot() { return snapshot("fine"); }
    }

    private static final int SERIES_CAP = TitanConfig.getInt("titan.metrics.series.max", 600);

    /** [ts, readyQueue, blockedInWaitingRoom, running, delayed] — sampled once per second. */
    private final Series queueSeries = new Series(SERIES_CAP);
    /** [ts, iterationMs] — one sample per dispatch-loop pass that actually placed a job. */
    private final Series dispatchSeries = new Series(SERIES_CAP);

    /**
     * Where the serialized dispatch section actually spends its time, per iteration:
     * {@code [ts, route, select, record, store, send]} in milliseconds.
     * <p>
     * {@link #dispatchSeries} gives the total, which tells you the loop got slower but not why.
     * These five are the phases every dispatch passes through — capability routing, worker
     * selection, span/bookkeeping, the store writes that sit on the dispatch path, and the
     * network hand-off to the worker. Only timestamps are taken; the loop's order and its
     * blocking behaviour are untouched.
     */
    private final Series dispatchBreakdown = new Series(SERIES_CAP);
    /** [ts, completedSinceLastSample, failedSinceLastSample] — throughput. */
    private final Series throughputSeries = new Series(SERIES_CAP, Roll.SUM);
    /**
     * [ts, latencyMs] per store operation.
     * <p>
     * Worth its own series because two of these writes sit inside {@link #runDispatchLoop} — a slow
     * store therefore slows cluster-wide dispatch, and nothing about the architecture makes that
     * guessable from the outside.
     */
    private final Series storeLatency = new Series(SERIES_CAP);

    private final java.util.concurrent.atomic.AtomicLong storeOps = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong storeErrors = new java.util.concurrent.atomic.AtomicLong();
    /**
     * Writes skipped because the store was disconnected.
     * <p>
     * The adapter fails open — {@code set()} returns early when not connected rather than throwing —
     * so an outage produces zero exceptions and the error counter stays at 0. This is the number
     * that actually says how much state was lost.
     */
    private final java.util.concurrent.atomic.AtomicLong storeDropped = new java.util.concurrent.atomic.AtomicLong();

    /** When a store write last succeeded. Answers "how stale is persisted state?". */
    private volatile long storeLastWriteOk = 0;

    private final Series storeReadLatency = new Series(SERIES_CAP);

    /**
     * Queue-wait percentiles sampled over time: [ts, p50, p95, p99].
     * <p>
     * The point-in-time block reports percentiles over the whole span log, which smooths away
     * when things got bad. Sampling them gives the shape of a degradation.
     */
    private final Series waitPercentiles = new Series(SERIES_CAP);

    /** Per-worker occupied slots over time, keyed host:port -> [ts, used, cap]. */
    private final Map<String, Series> workerLoadSeries = new ConcurrentHashMap<>();

    /**
     * How long a departed worker's load series is kept after it stops being sampled. Its tail is
     * useful immediately after a reclaim; past this it is only noise that inflates any total
     * computed from the map.
     */
    private static final long WORKER_SERIES_GRACE_MS =
            TitanConfig.getInt("titan.metrics.worker.grace.ms", 120_000);

    /**
     * The worker keys sampled on the last tick, so the payload can say which load series belong
     * to nodes that are still registered. Total capacity must come from the queue series, which
     * is sampled from the registry — never from summing this map, which may hold departed nodes.
     */
    private volatile java.util.Set<String> lastLiveWorkerKeys = java.util.Collections.emptySet();
    private final java.util.concurrent.atomic.AtomicLong storeReads = new java.util.concurrent.atomic.AtomicLong();

    private volatile long storeLastErrorAt = 0;
    private volatile String storeLastError = "";
    /** Per-worker heartbeat round-trip: workerKey -> [ts, rttMs]. */
    private final Map<String, Series> heartbeatRtt = new ConcurrentHashMap<>();
    /** [ts, type, detail] where type is an ordinal; kept as strings for the UI. */
    private final java.util.Deque<String> scalerEvents = new java.util.concurrent.ConcurrentLinkedDeque<>();

    /** Lifetime counts, so the panel can show totals rather than only the recent window. */
    private final java.util.concurrent.atomic.AtomicLong scaleUpCount = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong descaleCount = new java.util.concurrent.atomic.AtomicLong();
    private volatile int peakWorkers = 0;

    private final java.util.concurrent.atomic.AtomicLong completedCounter = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong failedCounter = new java.util.concurrent.atomic.AtomicLong();
    private long lastCompletedMark = 0, lastFailedMark = 0;

    /** Records a scaler decision so it can be drawn as a marker on the metric charts. */
    private void recordScalerEvent(String type, String detail) {
        if ("SCALE_UP".equals(type)) scaleUpCount.incrementAndGet();
        if ("DESCALE".equals(type)) descaleCount.incrementAndGet();
        scalerEvents.addLast(System.currentTimeMillis() + "|" + type + "|" + detail);
        while (scalerEvents.size() > 200) scalerEvents.pollFirst();
    }
    private final Map<String, Worker> liveServiceMap = new ConcurrentHashMap<>();

    /**
     * What each live service is, beyond "it exists": {@code serviceId -> "host:port:sinceMs"}.
     * <p>
     * {@link #liveServiceMap} answers which worker hosts a service, and the store holds its
     * address — but reading the address back costs one store round trip per service, and neither
     * records when it came up. Services are the long-running half of the workload and the
     * control plane could only show a count, so this keeps the address and the start time in
     * memory alongside the host.
     */
    private final Map<String, String> serviceInfo = new ConcurrentHashMap<>();

    // For history maintenance of (tasks all types).
    private final Map<String, Integer> workerCompletionStats = new ConcurrentHashMap<>();
    private final Map<String, java.util.Deque<Job>> workerRecentHistory = new ConcurrentHashMap<>();

    // Map to hold Active Job Objects for Async Retries
    private final Map<String, Job> runningJobs = new ConcurrentHashMap<>();
    private final Map<String, Job> dagWaitingRoom;

    // Job IDs that were explicitly cancelled by the user.  When a stale FAILED
    // callback arrives from the worker (after the process was killed), we discard
    // it here rather than triggering the normal retry / DEAD path.
    private final Set<String> cancelledJobs = ConcurrentHashMap.newKeySet();

    /**
     * Why a queued job could not be placed on its most recent dispatch attempt.
     * <p>
     * Without this, a job needing a capability no worker offers is indistinguishable from one that
     * is merely next in line — it just sits there. This is the scheduler equivalent of Kubernetes'
     * "0/3 nodes are available" message, and it is the first thing you want when work isn't moving.
     */
    private final Map<String, String> pendingReasons = new ConcurrentHashMap<>();

    /**
     * Jobs that cannot run because <em>no worker offers their required capability</em>, keyed by that
     * capability.
     * <p>
     * This is deliberately separate from the saturation case. Saturation is transient — a slot frees
     * within seconds, so re-queuing and waiting on {@link #capacitySignal} is right. A missing
     * capability is a supply problem that resolves only when a matching worker registers, so
     * re-queuing just spins: measured at 367 dispatch-loop passes for two impossible jobs, each
     * costing the single-threaded loop a 2s park. Parked jobs leave {@link #taskQueue} entirely and
     * are re-admitted by {@link #registerWorker} — event-driven, like {@link #unlockChildren}.
     */
    private final Map<String, java.util.Deque<Job>> parkingLot = new ConcurrentHashMap<>();

    /** When each job was parked, for the optional deadline. */
    private final Map<String, Long> parkedAt = new ConcurrentHashMap<>();

    /**
     * How long a job may stay parked before being failed to the DLQ. {@code 0} disables the
     * deadline, which is the default: a job parked for a GPU worker you intend to start later is
     * legitimately waiting, and it is visible in the metrics the whole time. Silently killing it is
     * worse than letting it sit where you can see it.
     */
    private static final long PARK_DEADLINE_MS =
            TitanConfig.getInt("titan.scheduler.unschedulable.deadline.ms", 0);

    /** Workers seen at least once, so re-registration isn't reported as a join. */
    private final Set<String> seenWorkers = ConcurrentHashMap.newKeySet();

    // AutoScaling declarations
    int MAX_WORKERS = 5;
    private volatile boolean scalingInProgress = false;
    private final ScheduledExecutorService scalerExecutor = Executors.newSingleThreadScheduledExecutor();

    /**
     * Runs service readiness probes off the dispatch thread.
     * <p>
     * {@link #runDispatchLoop()} is single-threaded, so probing inline stalls every other job in
     * the cluster for the length of the probe. Deploys hand the wait here and return
     * {@code JOB_ACCEPTED}; this pool owns the completion, exactly as {@link #handleJobCallback}
     * owns it for async task callbacks.
     */
    private final ScheduledExecutorService readinessExecutor = Executors.newScheduledThreadPool(
            TitanConfig.getInt("titan.service.ready.threads", 4),
            r -> {
                Thread t = new Thread(r, "titan-readiness");
                t.setDaemon(true);
                return t;
            });
    // Remember bad ports for avoiding during scaling
    private final Set<Integer> portBlacklist = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // Dispatch backpressure. The single-threaded dispatch loop parks here when every
    // capable worker is saturated, and is woken the moment a slot frees. The wait()
    // timeouts are a safety net for a missed notify (a slot freeing between the
    // saturation check and the wait) — that case degrades to the old poll interval
    // rather than deadlocking. Never held while any Worker monitor is taken.
    // Capacity is signalled through a 1-slot queue rather than wait/notify: a token
    // offered BEFORE the dispatch loop starts waiting is RETAINED, so a slot that
    // frees between the saturation check and the wait still wakes the loop. A bare
    // notifyAll() lands on an empty wait set in that window and is lost, which makes
    // the loop sit out its full timeout — measurably, on most cycles.
    // Capacity 1 coalesces bursts: N releases wake the loop once, which is all it needs.
    private final BlockingQueue<Object> capacitySignal = new LinkedBlockingQueue<>(1);
    private static final Object CAPACITY_TOKEN = new Object();

    private void signalCapacity() {
        capacitySignal.offer(CAPACITY_TOKEN);   // non-blocking; drops if one is pending
    }

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

        String rHost = TitanConfig.get("titan.redis.host", "localhost");
        int rPort = TitanConfig.getInt("titan.redis.port", 6379);
        this.redis = new TitanJRedisAdapter(redisHost, redisPort);

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
                reconcileWorkerSet();
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

        sweepOldSpanFiles();
        // Daily sweep so a long-lived Master does not accumulate history forever.
        scalerExecutor.scheduleAtFixedRate(this::sweepOldSpanFiles, 1, 24 * 60, TimeUnit.MINUTES);
        startMetricsSampler();

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
    /**
     * Samples cluster depth and throughput once a second into bounded ring buffers.
     * <p>
     * The stats endpoint only ever reported instantaneous values, so there was no way to see a
     * queue building up or draining — only its height at the moment you looked. These rings are the
     * time axis for those numbers, and cost one shallow read per second.
     */
    public void startMetricsSampler() {
        scalerExecutor.scheduleAtFixedRate(() -> {
            try {
                long now = System.currentTimeMillis();
                long blocked = dagWaitingRoom.size();
                long delayed = waitingRoom.size();
                int wCount = 0, slots = 0;
                java.util.Set<String> liveKeys = new java.util.HashSet<>();
                int occupied = 0;
                for (Worker sw : workerRegistry.getWorkers()) {
                    wCount++; slots += sw.getMaxCap();
                    occupied += sw.getCurrentLoad();
                    String key = sw.host() + ":" + sw.port();
                    liveKeys.add(key);
                    workerLoadSeries.computeIfAbsent(key, k -> new Series(SERIES_CAP))
                                    .add(now, sw.getCurrentLoad(), sw.getMaxCap());
                }
                // A reclaimed ephemeral worker stopped being sampled but its series stayed, keeping
                // its last reading forever. That made a dead node indistinguishable from an idle
                // one on the utilisation chart, and made the map unusable for capacity: summing it
                // counted slots belonging to workers that no longer exist. Keep departed nodes for
                // a grace window — their tail is exactly what you want right after a reclaim —
                // then drop them.
                workerLoadSeries.keySet().removeIf(k -> {
                    if (liveKeys.contains(k)) return false;
                    java.util.List<long[]> pts = workerLoadSeries.get(k).snapshot("fine");
                    long last = pts.isEmpty() ? 0 : pts.get(pts.size() - 1)[0];
                    return now - last > WORKER_SERIES_GRACE_MS;
                });
                // Same orphaning applies to heartbeat: heartbeatRtt is only cleared when a
                // heartbeat FAILS, but a descaled worker never fails one — the scaler removes it
                // from the registry directly — so its RTT line lingered on the chart forever.
                heartbeatRtt.keySet().removeIf(k -> {
                    if (liveKeys.contains(k)) return false;
                    java.util.List<long[]> pts = heartbeatRtt.get(k).snapshot("fine");
                    long last = pts.isEmpty() ? 0 : pts.get(pts.size() - 1)[0];
                    return now - last > WORKER_SERIES_GRACE_MS;
                });
                lastLiveWorkerKeys = liveKeys;
                reconcileOrphanedJobs(liveKeys);

                // Percentiles over a recent slice of the span log, so the series reflects "lately"
                // rather than the lifetime average.
                java.util.List<Long> recentWaits = new java.util.ArrayList<>();
                long since = now - 60_000;
                for (TaskExecution te : spanLog) {
                    if (te.enqueuedAt > 0 && te.startTime >= since) recentWaits.add(te.getQueueWaitMs());
                }
                java.util.Collections.sort(recentWaits);
                waitPercentiles.add(now, pct(recentWaits, 0.50), pct(recentWaits, 0.95), pct(recentWaits, 0.99));
                // Fields 5 and 6 let the scaling chart draw capacity against demand on one axis.
                // Field 7 is occupied slots, summed from the workers themselves. It is NOT the
                // same number as `running`: runningJobs holds a job until its completion callback
                // arrives, so a job whose worker was reclaimed mid-flight stays counted there and
                // running can exceed the fleet's total slots. Occupancy comes from the live
                // workers' own counters, so it is the honest numerator for saturation — and the
                // gap between the two is itself the signal that work has been orphaned.
                queueSeries.add(now, taskQueue.size(), blocked, runningJobs.size(), delayed,
                        wCount, slots, occupied);
                peakWorkers = Math.max(peakWorkers, workerRegistry.getWorkers().size());
                expireParkedJobs();
                flushSpansToDisk();

                long c = completedCounter.get(), f = failedCounter.get();
                throughputSeries.add(now, c - lastCompletedMark, f - lastFailedMark);
                lastCompletedMark = c;
                lastFailedMark = f;
            } catch (Exception e) {
                System.err.println("[WARN] Metrics sample failed: " + e.getMessage());
            }
        }, 1, 1, TimeUnit.SECONDS);
        System.out.println("[INFO] Metrics sampler started (1s interval)");
    }

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

            // Scaling must be driven by work that exists, not by worker state alone. An empty pool
            // was previously reported as "saturated", so a momentary gap — a descale, a missed
            // heartbeat — spawned a node with nothing queued. Capacity is only short if something
            // is actually waiting for it.
            int queuedDemand = taskQueue.size();
            boolean hasDemand = queuedDemand > 0;

            boolean generalPoolSaturated;
            if (generalWorkers.isEmpty()) {
                // No general worker at all is only a problem when there is work to place.
                generalPoolSaturated = hasDemand;
            } else {
                generalPoolSaturated = generalWorkers.stream().allMatch(Worker::isSaturated);
            }

            long busyCount = allWorkers.stream().filter(w -> w.currentJobId != null).count();
            int totalCount = allWorkers.size();
            int totalUsedSlots = allWorkers.stream().mapToInt(Worker::getCurrentLoad).sum();
            int totalAvailableSlots = allWorkers.stream().mapToInt(Worker::getMaxCap).sum();;
            System.out.println("[SCALER] Cluster Pressure: " + totalUsedSlots + "/" + totalAvailableSlots);

            if (hasDemand && generalPoolSaturated && totalCount < MAX_WORKERS){
               scalingInProgress = true;
               recordScalerEvent("SCALE_UP", queuedDemand + " job(s) queued, all "
                       + Math.max(generalWorkers.size(), 0) + " GENERAL worker(s) saturated");
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
                    recordScalerEvent("DESCALE", "idle worker :" + idleTarget.port());
                    safeRedisSrem("system:live_workers", idleTarget.host() + ":" + idleTarget.port());
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

    /** How long a job may be assigned to an unregistered worker before it is treated as stranded. */
    /** How many pending reasons the payload carries; the total is reported alongside. */
    private static final int PENDING_REASON_CAP =
            TitanConfig.getInt("titan.metrics.pending.reasons.max", 25);

    private static final long ORPHAN_GRACE_MS =
            TitanConfig.getInt("titan.scheduler.orphan.grace.ms", 30_000);

    /**
 * Safety net for in-flight work whose worker is no longer registered.
 * <p>
 * {@link #recoverJobsFrom(Worker)} handles the case the Master detects &mdash; a failed
 * heartbeat. A worker can leave the registry by other routes: reclaimed by the scaler,
 * decommissioned on command, or removed by a path not yet accounted for. This sweep catches all
 * of them by checking the invariant directly rather than by hooking every exit: no job may be
 * running on a worker that is not in the fleet.
 * <p>
 * The grace window exists so a worker that is mid-registration, or a completion callback still in
 * flight, is not mistaken for a dead one. Runs on the metrics sampler, never on the dispatch
 * thread.
 *
 * @param liveKeys {@code host:port} of every worker currently registered.
 */
    private void reconcileOrphanedJobs(java.util.Set<String> liveKeys) {
        long now = System.currentTimeMillis();
        java.util.List<Job> orphans = new java.util.ArrayList<>();
        for (Map.Entry<String, Job> e : runningJobs.entrySet()) {
            TaskExecution rec = executionHistory.get(e.getKey());
            if (rec == null || rec.assignedWorker == null || rec.endTime != 0) continue;
            String key = rec.assignedWorker.host() + ":" + rec.assignedWorker.port();
            if (liveKeys.contains(key)) continue;
            if (now - rec.startTime < ORPHAN_GRACE_MS) continue;
            orphans.add(e.getValue());
        }
        if (orphans.isEmpty()) return;

        System.err.println("[RECOVERY] " + orphans.size() + " job(s) still counted running on "
                + "workers that have left the fleet. Recovering.");
        for (Job job : orphans) {
            runningJobs.remove(job.getId());
            TaskExecution rec = executionHistory.get(job.getId());
            if (rec != null && rec.endTime == 0) {
                String w = rec.assignedWorker != null
                        ? rec.assignedWorker.host() + ":" + rec.assignedWorker.port() : "unknown";
                rec.fail("Worker " + w + " left the fleet while this job was running."
                        + " The job did not report a result; it is being retried.");
                persistSpan(rec);
            }
            handleJobFailure(job);
        }
        signalCapacity();
    }

    /**
 * Recovers work that was in flight on a worker the Master has just declared dead.
 * <p>
 * A job enters {@link #runningJobs} at dispatch and leaves it when its completion callback
 * arrives. If the worker dies first, that callback never comes, and nothing else removed the
 * entry &mdash; so the job stayed "RUNNING" forever: the running count stayed inflated above the
 * fleet's occupied slots, its span never closed, the store kept reporting RUNNING, and any child
 * waiting on it blocked permanently. One lost ephemeral worker stranded four jobs for eleven
 * hours and left a dependent sink node waiting on a parent that could never complete.
 * <p>
 * Runs on the heartbeat thread, never on the dispatch thread. Each stranded job has its span
 * closed as FAILED and is then handed to {@link #handleJobFailure(Job)}, so bounded retries and
 * the dead-letter queue apply exactly as they would for any other failure &mdash; a worker dying
 * is not a special kind of failure, it is a failure whose evidence arrives late.
 *
 * @param deadWorker the worker that stopped answering heartbeats.
 */
    private void recoverJobsFrom(Worker deadWorker) {
        if (deadWorker == null) return;
        java.util.List<Job> stranded = new java.util.ArrayList<>();
        for (Map.Entry<String, Job> e : runningJobs.entrySet()) {
            TaskExecution rec = executionHistory.get(e.getKey());
            if (rec == null || rec.assignedWorker == null) continue;
            if (rec.assignedWorker.host().equals(deadWorker.host())
                    && rec.assignedWorker.port() == deadWorker.port()) {
                stranded.add(e.getValue());
            }
        }
        if (stranded.isEmpty()) return;

        System.err.println("[RECOVERY] Worker " + deadWorker.host() + ":" + deadWorker.port()
                + " died holding " + stranded.size() + " in-flight job(s). Recovering.");
        for (Job job : stranded) {
            runningJobs.remove(job.getId());
            TaskExecution rec = executionHistory.get(job.getId());
            if (rec != null && rec.endTime == 0) {
                rec.fail("Worker " + deadWorker.host() + ":" + deadWorker.port()
                        + " died while this job was running. The job did not report a result;"
                        + " it is being retried.");
                persistSpan(rec);
            }
            System.err.println("[RECOVERY] Re-queueing " + job.getId());
            handleJobFailure(job);
        }
        // The dispatch loop may be parked waiting for capacity; the re-queued work needs it awake.
        signalCapacity();
    }

    /**
 * Sends heartbeat requests to all registered workers to verify their liveness and update their load status.
 * Workers that do not respond are marked as dead. Responding workers update their last seen timestamp and load metrics.
 * This method also updates Redis with the status of live workers.
 */
    public void checkHeartBeat(){
        System.out.println("Sending Heartbeat");
        for(Worker worker: workerRegistry.getWorkers()){

            long hbStart = System.currentTimeMillis();
            String result = schedulerClient.sendRequest(
                    worker.host(),
                    worker.port(),
                    TitanProtocol.OP_HEARTBEAT,
                    ""
            );
            // Round-trip to this worker. The only continuous network-health signal the Master has:
            // a link degrading toward failure shows here well before markWorkerDead fires.
            heartbeatRtt.computeIfAbsent(worker.host() + ":" + worker.port(), k -> new Series(SERIES_CAP))
                        .add(hbStart, System.currentTimeMillis() - hbStart, result == null ? 0 : 1);

            if(result  == null){
                recordScalerEvent("WORKER_LOST", worker.host() + ":" + worker.port() + " heartbeat failed");
                // system:live_workers was only ever added to, so dead nodes accumulated in the
                // store forever and its worker count drifted above the Master's.
                safeRedisSrem("system:live_workers", worker.host() + ":" + worker.port());
                seenWorkers.remove(worker.host() + ":" + worker.port());
                heartbeatRtt.remove(worker.host() + ":" + worker.port());
                workerRegistry.markWorkerDead(worker.host(), worker.port());
                recoverJobsFrom(worker);
            } else if(result.startsWith("PONG")){
                String workerKey = worker.host() + ":" + worker.port();

                safeRedisSadd("system:live_workers", workerKey);

                worker.updateLastSeen();
                String[] parts = result.split("\\|");
                if (parts.length > 1) {
                    int load = Integer.parseInt(parts[1]);
                    worker.setCurrentLoad(load);

                    safeRedisSet("worker:" + workerKey + ":load", String.valueOf(worker.getCurrentLoad()));

                    if(load > 0){
                        System.out.println("Worker " + worker.port() + "Has load" + worker.getCurrentLoad());
                    }
                }
                if (parts.length > 2) {
                    int maxCapacity = Integer.parseInt(parts[2]);
                    worker.setMaxCap(maxCapacity);
                }
                // Appended fields, read only when present: a worker built before this change
                // sends three parts and is parsed exactly as it always was. Malformed values are
                // treated as "unknown" rather than being allowed to kill the heartbeat.
                if (parts.length > 5) {
                    try {
                        worker.setHostStats(Integer.parseInt(parts[3].trim()),
                                            Integer.parseInt(parts[4].trim()),
                                            Integer.parseInt(parts[5].trim()));
                    } catch (NumberFormatException ignored) {
                        worker.setHostStats(-1, -1, -1);
                    }
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

    /**
 * Registers a new worker with the scheduler. This method adds the worker to the registry,
 * clears any `scalingInProgress` flag, and removes the port from the blacklist.
 * It also handles the 'promotion' of auto-scaled worker deployment jobs, marking the parent worker as idle.
 *
 * @param host The hostname or IP address of the worker.
 * @param port The port number the worker is listening on.
 * @param capability A string describing the worker's capabilities (e.g., "GENERAL", "GPU").
 * @param isPermanent {@code true} if the worker is a permanent part of the cluster and should not be scaled down, {@code false} otherwise.
 */
    public synchronized void registerWorker(String host, int port, String capability, boolean isPermanent) {
        this.workerRegistry.addWorker(host, port, capability, isPermanent);
        admitParkedFor(capability);
        this.scalingInProgress = false;
        this.portBlacklist.remove(port);
        signalCapacity();

//        System.out.println("[DEBUG] Attempting promotion for incoming worker at " + host + ":" + port);
        boolean isNew = !seenWorkers.contains(host + ":" + port);
        if (isNew) {
            seenWorkers.add(host + ":" + port);
            recordScalerEvent("WORKER_JOIN", host + ":" + port + (isPermanent ? " permanent" : " ephemeral"));
        }
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

    /**
 * Retrieves the map of jobs currently waiting for their DAG dependencies to be met.
 *
 * @return A map where keys are job IDs and values are the {@link Job} instances waiting on dependencies.
 */
    /**
     * Returns every parked job to the dispatch queue now that a worker offering {@code capability}
     * exists. Called on registration, so admission is driven by the cluster event that actually
     * changes the answer rather than by polling.
     *
     * @param capability The capability the newly registered worker provides.
     */
    private void admitParkedFor(String capability) {
        java.util.Deque<Job> waiting = parkingLot.remove(capability);
        if (waiting == null || waiting.isEmpty()) return;

        int n = 0;
        for (Job j : waiting) {
            if (cancelledJobs.contains(j.getId())) continue;
            parkedAt.remove(j.getId());
            pendingReasons.remove(j.getId());
            taskQueue.offer(j);
            n++;
        }
        if (n > 0) {
            System.out.println("[PARK] Released " + n + " job(s) waiting for " + capability);
            recordScalerEvent("PARK_RELEASE", n + " job(s) for " + capability);
            signalCapacity();
        }
    }

    /** Total jobs currently parked awaiting a capability. */
    private int parkedCount() {
        int n = 0;
        for (java.util.Deque<Job> d : parkingLot.values()) n += d.size();
        return n;
    }

    /**
     * Fails jobs that have been parked past {@link #PARK_DEADLINE_MS}. No-op when the deadline is
     * disabled, which is the default.
     */
    private void expireParkedJobs() {
        if (PARK_DEADLINE_MS <= 0) return;
        long now = System.currentTimeMillis();

        for (Map.Entry<String, java.util.Deque<Job>> e : parkingLot.entrySet()) {
            java.util.Iterator<Job> it = e.getValue().iterator();
            while (it.hasNext()) {
                Job j = it.next();
                Long since = parkedAt.get(j.getId());
                if (since == null || now - since < PARK_DEADLINE_MS) continue;

                it.remove();
                parkedAt.remove(j.getId());
                String why = pendingReasons.remove(j.getId());
                System.err.println("[PARK] " + j.getId() + " exceeded the unschedulable deadline. -> DLQ");

                j.setStatus(Job.Status.DEAD);
                safeRedisSet("job:" + j.getId() + ":status", "DEAD");
                safeRedisSrem("system:active_jobs", j.getId());
                deadLetterQueue.offer(j);
                recordScalerEvent("PARK_EXPIRED", j.getId() + " — " + (why == null ? "unschedulable" : why));
                cancelChildren(j.getId());
            }
        }
        parkingLot.entrySet().removeIf(e -> e.getValue().isEmpty());
    }

    public Map<String, Job> getDAGWaitingRoom(){
        return dagWaitingRoom;
    }

    /**
 * Submits a {@link Job} to the scheduler for processing. The job's state is persisted to Redis.
 * If the job has dependencies, it's placed in the DAG waiting room. If it has a scheduled time in the future,
 * it's placed in the waiting room. Otherwise, it's added directly to the active task queue.
 *
 * @param job The {@link Job} object to be submitted.
 */
    public void submitJob(Job job){
        // Persist to Redis to act as WAL
        // This will be the basis for recovery
        safeRedisSet("job:" + job.getId() + ":payload", job.getPayload());
        safeRedisSet("job:" + job.getId() + ":status", "PENDING");
        safeRedisSet("job:" + job.getId() + ":priority", String.valueOf(job.getPriority()));
        safeRedisSet("job:" + job.getId() + ":delay", String.valueOf(job.getScheduledTime()));
        safeRedisSadd("system:active_jobs", job.getId());

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
    /** @return Read latency samples, for the store health panel. */
    private void noteRead(long t0) {
        storeReads.incrementAndGet();
        storeReadLatency.add(t0, System.currentTimeMillis() - t0);
    }

    public String redisKVGet(String key){
        long t0 = System.currentTimeMillis();
        try {
            return redis.get(key);
        } catch (Exception e) {
            System.err.println("[INFO][FAILED][RECOVERY] Failed to fetch active jobs: " + e.getMessage());
            return null;
        } finally {
            // Reads sit on the status-polling path, writes on the dispatch path — different blast
            // radius, so they are measured separately.
            noteRead(t0);
        }
    }

    /**
 * Safely sets a key-value pair in Redis, catching and logging any IOException.
 *
 * @param key The key to set.
 * @param value The value to associate with the key.
 */
    /** A store call that may throw {@link IOException}. */
    private interface StoreCall { void run() throws IOException; }

    /**
     * Runs a store operation, recording its latency and counting it.
     *
     * @param call The operation.
     * @throws IOException Propagated so the existing wrappers keep their behaviour.
     */
    private void timedStore(StoreCall call) throws IOException {
        if (!redis.isConnected()) {
            storeDropped.incrementAndGet();
            return;                     // nothing to time; the write is lost either way
        }
        long t0 = System.currentTimeMillis();
        try {
            call.run();
        } finally {
            storeOps.incrementAndGet();
            storeLastWriteOk = System.currentTimeMillis();
            storeLatency.add(t0, System.currentTimeMillis() - t0);
        }
    }

    /**
     * Records a store failure. These were previously only printed, so an outage was invisible to
     * every UI while the cluster carried on scheduling.
     *
     * @param what Short description of the failed operation.
     * @param e The failure.
     */
    private void noteStoreError(String what, Exception e) {
        storeErrors.incrementAndGet();
        storeLastErrorAt = System.currentTimeMillis();
        storeLastError = what + ": " + e.getMessage();
        System.err.println("[WARN] Store op failed — " + storeLastError);
    }

    private void safeRedisSet(String key, String value) {
        try {
            timedStore(() -> redis.set(key, value));
        } catch (IOException e) {
            noteStoreError("SET " + key, e);
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
        try {
            timedStore(() -> redis.sadd(key, member));
        } catch (IOException e) {
            noteStoreError("SADD " + key, e);
        }
    }

    /**
 * Safely removes a member from a Redis set, catching and logging any exceptions.
 *
 * @param key The key of the set.
 * @param member The member to remove from the set.
 */
    private void safeRedisSrem(String key, String member) {
        try {
            redis.srem(key, member);
        } catch (Exception e) { // Catch Exception broadly as srem isn't in interface yet maybe
            System.err.println("[WARN] Redis SREM failed for " + key + ": " + e.getMessage());
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
    /**
     * Clears the store's worker set at boot.
     * <p>
     * {@code system:live_workers} is maintained by heartbeat and worker-death events. If the Master
     * dies ungracefully those events never fire, so the set keeps naming nodes from the previous
     * life and the store's fleet count drifts permanently above reality. A fresh Master has no
     * workers by definition, and every live one re-registers within 30s, so the safe reconciliation
     * is to start from empty.
     */
    private void reconcileWorkerSet() {
        try {
            java.util.Set<String> stale = safeRedisSMembers("system:live_workers");
            if (stale == null || stale.isEmpty()) return;
            for (String k : stale) safeRedisSrem("system:live_workers", k);
            System.out.println("[RECOVERY] Cleared " + stale.size()
                    + " stale worker entr" + (stale.size() == 1 ? "y" : "ies") + " from the store.");
        } catch (Exception e) {
            System.err.println("[WARN] Worker-set reconciliation failed: " + e.getMessage());
        }
    }

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

                Job job = new Job(payload, priority, remainingDelay);
                job.setId(jobId);
                job.setStatus(Job.Status.PENDING); // Force reset to Pending

                if ("COMPLETED".equals(status) || "DEAD".equals(status)) {
                    // Should have been removed, but if it's here, clean it.
                    safeRedisSrem("system:active_jobs", jobId);
                } else {
                    System.out.println("[INFO][RECOVERY] Restoring Job " + jobId + " (Was " + status + ")");
                    // We bypass submitJob() to avoid writing to Redis again needlessly but we need the queue logic.
                    if (remainingDelay > 0) {
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
                // Measured from the moment a job leaves the queue to the moment dispatch returns.
                // This is the serialized section: while it runs, nothing else in the cluster is dispatched.
                long iterStart = System.currentTimeMillis();
                System.out.println("DEBUG: Processing Job ID: " + job.getId());
                job.setStatus(Job.Status.RUNNING);
//                history.put(job.getId(), job.getStatus());
                System.out.println(" Job Processing: " + job);

                String reqTaskSkill = extractSkillRequirement(job);

                System.out.println("[DISPATCH] Job " + job.getId() + " requires: [" + reqTaskSkill + "]");

                List<Worker> availableWorkers = workerRegistry.getWorkersByCapability(reqTaskSkill);
                long tRouted = System.currentTimeMillis();

                if (availableWorkers.isEmpty()) {
                    // Supply problem, not pressure: nothing will change until a worker with this
                    // capability registers. Park it out of the queue instead of spinning on it.
                    pendingReasons.put(job.getId(), "no worker registered with capability " + reqTaskSkill);
                    job.setStatus(Job.Status.PENDING);
                    safeRedisSet("job:" + job.getId() + ":status", "PENDING");

                    parkingLot.computeIfAbsent(reqTaskSkill, k -> new java.util.concurrent.ConcurrentLinkedDeque<>())
                              .addLast(job);
                    parkedAt.putIfAbsent(job.getId(), System.currentTimeMillis());
                    System.out.println("[PARK] " + job.getId() + " waiting for a worker with capability "
                            + reqTaskSkill + " (parked: " + parkedCount() + ")");
                    continue;
                }

                Worker selectedWorker = selectBestWorker(job, availableWorkers);
                long tSelected = System.currentTimeMillis();

                if (selectedWorker == null) {
                    pendingReasons.put(job.getId(), availableWorkers.size() + " worker(s) match "
                            + reqTaskSkill + " but all are at capacity");
                    System.out.println("All workers SATURATED or unavailable. Re-queueing job.");
                    job.setStatus(Job.Status.PENDING);
                    taskQueue.put(job); // Use put for blocking
                    capacitySignal.poll(1000, TimeUnit.MILLISECONDS);
                    continue;
                }
                pendingReasons.remove(job.getId());
                selectedWorker.incrementCurrentLoad();
                TaskExecution record = new TaskExecution(job.getId(), selectedWorker,
                        job.getDependenciesIds(), job.getScheduledTime(), job.getRetryCount() + 1,
                        job.getPriority());
                spanLog.addLast(record);
                while (spanLog.size() > MAX_SPANS) spanLog.pollFirst();
                executionHistory.put(job.getId(), record);
                runningJobs.put(job.getId(), job);
                long tRecorded = System.currentTimeMillis();

                safeRedisSet("job:" + job.getId() + ":status", "RUNNING");
                safeRedisSet("job:" + job.getId() + ":worker", String.valueOf(selectedWorker.port()));
                long tStored = System.currentTimeMillis();

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
                            signalCapacity();
                        }

                    dispatchSeries.add(System.currentTimeMillis(), System.currentTimeMillis() - iterStart);
                    recordDispatchPhases(iterStart, tRouted, tSelected, tRecorded, tStored);

                } catch (Exception e){
                    dispatchSeries.add(System.currentTimeMillis(), System.currentTimeMillis() - iterStart);
                    recordDispatchPhases(iterStart, tRouted, tSelected, tRecorded, tStored);
                    System.err.println("[FAIL] Job " + job.getId() + " Error: " + e.getMessage());
                    record.fail(e.getMessage());
                    persistSpan(record);

                    runningJobs.remove(job.getId());

                    if (selectedWorker != null) {
                        selectedWorker.currentJobId = null;
                        selectedWorker.decrementCurrentLoad();
                        signalCapacity();
                        String wKey = String.valueOf(selectedWorker.port());
                        java.util.Deque<Job> errHist = workerRecentHistory.computeIfAbsent(wKey, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
                        errHist.removeIf(j -> j.getId().equals(job.getId()));
                        errHist.add(job);
                        if (errHist.size() > 10) errHist.removeFirst();
                    }

                    if (e.getMessage().contains("SATURATED")) {
                        System.out.println("[WARN] Worker " + selectedWorker.port() + " is Saturated (Optimistic check failed). Penalizing.");
                        selectedWorker.setCurrentLoad(99);
                    }


                    // We fail the job here and not give it retry if its a deployment issue
                    // A service that never bound its port will not bind on a retry either, and each
                    // attempt burns the full readiness window. Treat it as deterministic.
                    if (e.getMessage().contains("ALREADY in use") || e.getMessage().contains("Deployment Rejected")
                            || e.getMessage().contains("never became reachable")) {
                        System.err.println("[FAIL-FAST] Non-recoverable error. Cancelling retries.");
                        job.setStatus(Job.Status.FAILED);
                        // Persist it: the dispatch loop already wrote RUNNING to the store, and
                        // skipping handleJobFailure() means nothing else will ever overwrite it.
                        // Without this the job reports RUNNING forever to get_job_status().
                        safeRedisSet("job:" + job.getId() + ":status", "FAILED");
                        safeRedisSrem("system:active_jobs", job.getId());
                        cancelChildren(job.getId());
                        // We do NOT call handleJobFailure(job) here, so it won't retry/become DEAD.
                    } else{
                        handleJobFailure(job);
//                    history.put(job.getId(), job.getStatus());
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
        // Don't retry a job that was explicitly cancelled by the user.
        if (cancelledJobs.contains(job.getId())) {
            return;
        }
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

            safeRedisSet("job:" + job.getId() + ":status", "DEAD");
            //  Remove from active set since it is dead
            safeRedisSrem("system:active_jobs", job.getId());

            if (record != null) record.status = Job.Status.DEAD;
//            history.put(job.getId(), Job.Status.DEAD);
            failedCounter.incrementAndGet();
            System.err.println("Job Moved to DLQ (Max Retries): " + job);
            this.deadLetterQueue.offer(job);
            cancelChildren(job.getId());
            // Surface DEAD in workerRecentHistory so the dashboard stats reflect the
            // failure.  Without this, the history only grows on completeJob(), leaving
            // stale COMPLETED entries from previous runs visible in the UI.
            if (record != null && record.assignedWorker != null) {
                String wKey = String.valueOf(record.assignedWorker.port());
                java.util.Deque<Job> dHist = workerRecentHistory.computeIfAbsent(wKey, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
                dHist.removeIf(j -> j.getId().equals(job.getId()));
                dHist.add(job);
                if (dHist.size() > 10) dHist.removeFirst();
            }

        } else{
            job.setStatus(Job.Status.FAILED);
            System.err.println("Job Failed. Retrying... (" + job.getRetryCount() + "/3)");
            job.setStatus(Job.Status.PENDING);

            safeRedisSet("job:" + job.getId() + ":status", "PENDING");
            // We leave the record as FAILED for now so history shows it failed
            //history.put(job.getId(), Job.Status.PENDING);
            taskQueue.offer(job);
        }
    }

    /**
     * Cancels a job by ID regardless of its current state (queued, waiting, or running).
     * <ul>
     *   <li>Pending in taskQueue → removed and marked CANCELLED.</li>
     *   <li>Waiting in dagWaitingRoom → removed, marked CANCELLED, children cascaded.</li>
     *   <li>Running on a worker → kill signal sent to worker, marked CANCELLED, children cascaded.</li>
     * </ul>
     *
     * @param jobId The full job ID (e.g., "DAG-hitl-train").
     * @return A status string for the caller ("CANCELLED" or "NOT_FOUND").
     */
    public String cancelJob(String jobId) {
        System.out.println("[CANCEL] Cancelling job: " + jobId);
        cancelledJobs.add(jobId);

        // A parked job is in neither taskQueue nor dagWaitingRoom, so it must be removed here too.
        for (java.util.Deque<Job> parked : parkingLot.values()) {
            if (parked.removeIf(j -> j.getId().equals(jobId))) {
                parkedAt.remove(jobId);
                pendingReasons.remove(jobId);
                safeRedisSet("job:" + jobId + ":status", "CANCELLED");
                cancelChildren(jobId);
                return "CANCELLED";
            }
        }

        // --- 1. Remove from pending task queue ---
        taskQueue.removeIf(j -> j.getId().equals(jobId));

        // --- 2. Remove from DAG waiting room ---
        Job waitingJob = dagWaitingRoom.remove(jobId);
        if (waitingJob != null) {
            waitingJob.setStatus(Job.Status.CANCELLED);
            TaskExecution ghostRecord = new TaskExecution(jobId, null);
            ghostRecord.status = Job.Status.CANCELLED;
            ghostRecord.endTime = System.currentTimeMillis();
            ghostRecord.output  = "Cancelled by user";
            executionHistory.put(jobId, ghostRecord);
            safeRedisSet("job:" + jobId + ":status", "CANCELLED");
            safeRedisSrem("system:active_jobs", jobId);
            cancelChildren(jobId);
            System.out.println("[CANCEL] Removed waiting job: " + jobId);
            return "CANCELLED";
        }

        // --- 3. Kill on worker if actively running ---
        Job runningJob = runningJobs.remove(jobId);
        TaskExecution record = executionHistory.get(jobId);

        if (record != null) {
            record.status  = Job.Status.CANCELLED;
            record.endTime = System.currentTimeMillis();
            if (record.assignedWorker != null) {
                record.assignedWorker.currentJobId = null;
                record.assignedWorker.decrementCurrentLoad();
                signalCapacity();
                try {
                    schedulerClient.sendRequest(
                            record.assignedWorker.host(),
                            record.assignedWorker.port(),
                            TitanProtocol.OP_CANCEL_JOB,
                            jobId);
                } catch (Exception e) {
                    System.err.println("[CANCEL] Could not reach worker for kill signal: " + e.getMessage());
                }
            }
        }

        if (runningJob != null) {
            runningJob.setStatus(Job.Status.CANCELLED);
            // Surface in workerRecentHistory so the dashboard shows CANCELLED
            if (record != null && record.assignedWorker != null) {
                String wKey = String.valueOf(record.assignedWorker.port());
                java.util.Deque<Job> hist = workerRecentHistory.computeIfAbsent(wKey, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
                hist.removeIf(j -> j.getId().equals(jobId));
                hist.add(runningJob);
                if (hist.size() > 10) hist.removeFirst();
            }
        }

        safeRedisSet("job:" + jobId + ":status", "CANCELLED");
        safeRedisSrem("system:active_jobs", jobId);
        cancelChildren(jobId);

        return runningJob != null ? "CANCELLED" : "NOT_FOUND";
    }

    /**
 * Processes callbacks from workers regarding the completion or failure of asynchronous jobs.
 * The payload typically contains the job ID, status (COMPLETED/FAILED), and an optional result message.
 * This method updates the job's status, clears the worker's active job flag, and triggers completion or failure handling.
 *
 * @param payload The callback string from the worker (e.g., "JOB-123|COMPLETED|Result: 5050").
 */
    public void handleJobCallback(String payload){
        // Payload: "JOB-123|COMPLETED|Result: 5050"
        String[] parts = payload.split("\\|", 3);
        if (parts.length < 2) return;

        String jobId = parts[0];
        String statusStr = parts[1];
        String result = (parts.length > 2) ? parts[2] : "";

        // Discard stale callbacks that arrive after the job was cancelled.
        if (cancelledJobs.remove(jobId)) {
            System.out.println("[CANCEL] Dropped stale callback for cancelled job: " + jobId);
            return;
        }

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
                        signalCapacity();
                    }
//                    if(job != null) job.setStatus(Job.Status.COMPLETED);
//                }
            } else {
                System.err.println("[ASYNC] [FAILED] Callback: Job " + jobId + " Failed.");
                record.fail(result);
                persistSpan(record);

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
        completedCounter.incrementAndGet();
        persistSpan(record);

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

            if (job != null) {
                java.util.Deque<Job> cHist = workerRecentHistory.computeIfAbsent(wKey, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
                cHist.removeIf(j -> j.getId().equals(job.getId()));
                cHist.add(job);
                if (cHist.size() > 10) cHist.removeFirst();
            }

            propagateAffinity(job.getId(), wKey);
        }
        unlockChildren(job.getId());
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

    /**
 * Executes a deployment sequence on a worker. This involves staging a file (e.g., a JAR or script)
 * and then starting it as a service on a specified port. It includes checks for port availability
 * and waits for the deployed service to become reachable.
 * Special handling is included for internal auto-scaling deployments.
 *
 * @param job The {@link Job} representing the deployment.
 * @param worker The {@link Worker} on which to deploy.
 * @param payload The deployment payload, including filename, base64 content, and optional target port.
 * @return A success message including the PID if available.
 * @throws Exception If staging fails, starting the service fails, or the deployed service does not become reachable.
 */
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
            if (startResp.contains("SERVICE_ALREADY_RUNNING")) {
                throw new RuntimeException("Deployment Rejected: a service with ID " + job.getId()
                        + " is already running on " + worker.host() + ". Stop it before redeploying.");
            }
            if (!startResp.contains("DEPLOYED_SUCCESS")) {
                throw new RuntimeException("Start failed. Expected DEPLOYED_SUCCESS, got: " + startResp);
            }

            String pid = startResp.contains("PID:") ? startResp.split("PID:")[1].trim() : "UNKNOWN";

            if (targetPort != -1) {
                // Hand the wait to the readiness pool and release the dispatch thread. Children
                // still unlock only once the port answers - that now happens from that pool.
                awaitServiceReadyAsync(job, worker, targetPort, pid);
                return "JOB_ACCEPTED";
            }

            // Portless deploy: nothing to probe, so complete inline as before.
            liveServiceMap.put(job.getId(), worker);
            recordServiceAddress(job.getId(), worker.host(), targetPort);
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

    /**
 * Checks if a worker is alive and reachable on a given host and port by attempting to open a socket connection.
 *
 * @param host The hostname or IP address of the worker.
 *   @param port The port number of the worker.
 * @return {@code true} if the worker is reachable, {@code false} otherwise.
 */
    private boolean isWorkerAlive(String host, int port) {
        try (Socket s = new Socket(host, port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * The total window {@link #awaitServiceReady} will wait, in seconds. Derived from the
     * configured attempt count and interval so log/error messages stay truthful if either is tuned.
     *
     * @return The readiness window in seconds.
     */
    private int readyWindowSeconds() {
        return (TitanConfig.getInt("titan.service.ready.attempts", 10)
                * TitanConfig.getInt("titan.service.ready.interval.ms", 2000)) / 1000;
    }

    /**
     * Blocks until a freshly started service is accepting TCP connections on {@code port}.
     * <p>
     * A worker returns {@code DEPLOYED_SUCCESS} as soon as the child process is spawned, which is
     * before the service has bound its port. Completing the deploy job at that moment would unlock
     * downstream DAG nodes that then race the service's startup. Gating on an actual connect makes
     * "deployed" mean "reachable".
     *
     * @param host  Host the service was started on.
     * @param port  Port the service is expected to listen on.
     * @param label Service/job ID, used only for logging.
     * @return {@code true} if the port became reachable within the window, {@code false} otherwise.
     */
    private boolean awaitServiceReady(String host, int port, String label) {
        int attempts = TitanConfig.getInt("titan.service.ready.attempts", 10);
        int intervalMs = TitanConfig.getInt("titan.service.ready.interval.ms", 2000);

        for (int i = 1; i <= attempts; i++) {
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
            if (isWorkerAlive(host, port)) {
                System.out.println("[OK] Service " + label + " reachable on port " + port
                        + " (attempt " + i + "/" + attempts + ")");
                return true;
            }
            System.out.println("[DEPLOY] Port " + port + " not ready... (Attempt " + i + "/" + attempts + ")");
        }
        return false;
    }

    /**
     * Records where a live service is reachable so consumer jobs can resolve it at runtime
     * instead of hard-coding a host and port.
     * <p>
     * The worker's own RPC port is not the service's listening port, so {@code liveServiceMap}
     * alone cannot answer "where is service X?". These keys close that gap.
     *
     * @param serviceId The service (job) ID.
     * @param host      Host the service is reachable on.
     * @param port      Port the service is listening on. Values &lt;= 0 are ignored (portless daemons).
     */
    private void recordServiceAddress(String serviceId, String host, int port) {
        if (port <= 0) return;
        // Keep the first start time across re-registrations: a service that re-announces itself
        // has not restarted its clock, and uptime resetting on every announce would be a lie.
        String existing = serviceInfo.get(serviceId);
        long since = System.currentTimeMillis();
        if (existing != null) {
            int cut = existing.lastIndexOf(':');
            if (cut > 0) {
                try {
                    since = Long.parseLong(existing.substring(cut + 1));
                } catch (NumberFormatException ignored) { /* keep now */ }
            }
        }
        serviceInfo.put(serviceId, host + ":" + port + ":" + since);
        safeRedisSet("service:" + serviceId + ":host", host);
        safeRedisSet("service:" + serviceId + ":port", String.valueOf(port));
        safeRedisSadd("system:live_services", serviceId);
        System.out.println("[DISCOVERY] Registered " + serviceId + " -> " + host + ":" + port);
    }

    /**
     * Removes a service's discovery record once it is no longer live.
     *
     * @param serviceId The service (job) ID.
     */
    /**
     * Waits for a freshly started service to become reachable on a background thread, then
     * completes or fails the deploy job.
     * <p>
     * The caller must return {@code JOB_ACCEPTED} so the dispatch loop leaves the job alone.
     * Ownership of the worker's load counter and the capacity signal transfers to this task.
     *
     * @param job        The deploy job awaiting readiness.
     * @param worker     The worker hosting the service.
     * @param targetPort The port the service must bind.
     * @param pid        PID string reported by the worker, for the result message.
     */
    private void awaitServiceReadyAsync(Job job, Worker worker, int targetPort, String pid) {
        readinessExecutor.submit(() -> {
            TaskExecution record = executionHistory.get(job.getId());
            try {
                if (awaitServiceReady(worker.host(), targetPort, job.getId())) {
                    liveServiceMap.put(job.getId(), worker);
                    recordServiceAddress(job.getId(), worker.host(), targetPort);
                    worker.currentJobId = null;
                    runningJobs.remove(job.getId());

                    if (record != null) {
                        completeJob(job, "DEPLOYED_SUCCESS PID:" + pid, record);
                    }
                    worker.decrementCurrentLoad();
                    signalCapacity();
                } else {
                    failServiceDeploy(job, worker, record, "Deployment Failed: " + job.getId()
                            + " started but port " + targetPort + " never became reachable after "
                            + readyWindowSeconds() + "s.");
                }
            } catch (Exception e) {
                failServiceDeploy(job, worker, record,
                        "Deployment Failed: readiness check errored for " + job.getId() + ": " + e.getMessage());
            }
        });
    }

    /**
     * Terminal-fails a deploy from the readiness thread, mirroring what the dispatch loop's catch
     * block would have done synchronously: release the worker, unblock the scaler, persist the
     * status, and cascade to children.
     * <p>
     * A service that never bound its port will not bind on a retry, so this is deliberately
     * terminal rather than routed through {@link #handleJobFailure}.
     *
     * @param job    The failed deploy job.
     * @param worker The worker it was dispatched to.
     * @param record Its execution record, may be {@code null}.
     * @param msg    Human-readable failure reason.
     */
    private void failServiceDeploy(Job job, Worker worker, TaskExecution record, String msg) {
        System.err.println("[FAIL] " + msg);

        if (record != null) { record.fail(msg); persistSpan(record); }
        runningJobs.remove(job.getId());

        if (worker != null) {
            worker.currentJobId = null;
            worker.decrementCurrentLoad();
            signalCapacity();
        }

        // Let the scaler pick a different port next cycle instead of deadlocking on the lock.
        if (job.getId().startsWith("WRK-")) {
            this.scalingInProgress = false;
        }

        job.setStatus(Job.Status.FAILED);
        safeRedisSet("job:" + job.getId() + ":status", "FAILED");
        safeRedisSrem("system:active_jobs", job.getId());

        // Surface it in the dashboard's per-worker history, same as completeJob does on success.
        if (worker != null) {
            String wKey = String.valueOf(worker.port());
            java.util.Deque<Job> hist = workerRecentHistory.computeIfAbsent(
                    wKey, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
            hist.removeIf(j -> j.getId().equals(job.getId()));
            hist.add(job);
            if (hist.size() > 10) hist.removeFirst();
        }

        cancelChildren(job.getId());
    }

    private void forgetServiceAddress(String serviceId) {
        serviceInfo.remove(serviceId);
        safeRedisSrem("system:live_services", serviceId);
        safeRedisSet("service:" + serviceId + ":host", "");
        safeRedisSet("service:" + serviceId + ":port", "");
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
 * and instructs the worker to start it as a detached service on a specified port.
 *
 * @param job The {@link Job} representing the archived service.
 * @param worker The {@link Worker} on which to start the service.
 * @param payload The payload containing the archive pointer, optional arguments, and target port.
 * @return The response from the worker, typically indicating deployment success.
 * @throws Exception If resolving the archive pointer fails or the service start command fails.
 */
    private String executeServiceArchive(Job job, Worker worker, String payload) throws Exception {
        // Payload Format: START_ARCHIVE_SERVICE | zip_name.zip/entry.py | args | port
        String[] parts = payload.split("\\|");

        String pointer = parts[1];
        String args = (parts.length > 2) ? parts[2] : "";
        String port = (parts.length > 3) ? parts[3] : "8085";

        AssetManager.ArchiveInfo fileInfo = AssetManager.resolvePointer(pointer);

        // Worker Protocol for Service Archive: SERVICE_ID | ENTRY_FILE | PORT | BASE64_ZIP
        String workerPayload = job.getId() + "|" + fileInfo.entryPoint + "|" + port + "|" + fileInfo.base64Content;

        int declaredPort = -1;
        try {
            declaredPort = Integer.parseInt(port.trim());
        } catch (NumberFormatException ignored) {
            // Portless service - nothing to reserve or probe.
        }

        // Reject a collision BEFORE starting anything. Without this the readiness probe below
        // would connect to whatever already owns the port and report a success that never happened.
        if (declaredPort > 0 && isWorkerAlive(worker.host(), declaredPort)) {
            throw new RuntimeException("Deployment Rejected: Port " + declaredPort
                    + " is ALREADY in use by another service.");
        }

        System.out.println("🚀 [ARCHIVE] Starting Service " + job.getId() + " on Port " + port);

        String response = sendExecuteCommand(worker, TitanProtocol.OP_START_SERVICE_ARCHIVE, workerPayload);

        // The worker refuses to start a second process under an existing service ID. That is not a
        // success - without this the Master would complete the job and unlock children regardless.
        if (response.contains("SERVICE_ALREADY_RUNNING")) {
            throw new RuntimeException("Deployment Rejected: a service with ID " + job.getId()
                    + " is already running on " + worker.host() + ". Stop it before redeploying.");
        }

        if (response.contains("DEPLOYED_SUCCESS")) {
            if (declaredPort > 0) {
                // DEPLOYED_SUCCESS here means "process spawned", not "service listening". Gate on a
                // real connect, off the dispatch thread.
                awaitServiceReadyAsync(job, worker, declaredPort, "DETACHED");
                return "JOB_ACCEPTED";
            }

            liveServiceMap.put(job.getId(), worker);
            worker.currentJobId = null; // Since the Services are detached
        }
        return response;
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

        // A null response is a transport failure, not a task failure. Reporting both as
        // "Worker Error: <response>" produced the useless "Worker Error: null" and made an
        // unreachable node look identical to a script that crashed.
        if (response == null) {
            String msg = "Worker unreachable: " + worker.host() + ":" + worker.port()
                    + " accepted no reply (process gone, port closed, or network down)."
                    + " The job never ran — this is not a script failure.";
            System.err.println("[FAIL] " + msg);
            // Evict it now rather than waiting for the next heartbeat, so retries are not sent
            // straight back to the same dead node.
            workerRegistry.markWorkerDead(worker.host(), worker.port());
            recordScalerEvent("WORKER_LOST", worker.host() + ":" + worker.port() + " no reply on dispatch");
            safeRedisSrem("system:live_workers", worker.host() + ":" + worker.port());
            heartbeatRtt.remove(worker.host() + ":" + worker.port());
            throw new RuntimeException(msg);
        }
        if (response.startsWith("ERROR") || response.startsWith("JOB_FAILED")) {
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
            if (response.contains("SUCCESS") || response.contains("STOPPED")) {
                liveServiceMap.remove(serviceId);
                forgetServiceAddress(serviceId);
            }
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

    /**
 * Unlocks child jobs that were dependent on a newly completed parent job.
 * It iterates through jobs in the DAG waiting room, resolves the dependency for the given parent ID,
 * and if all dependencies for a child job are met, it moves that child job to the active task queue.
 *
 * @param parentId The ID of the parent job that has just completed.
 */
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

    /**
 * Recursively cancels child jobs whose parent job has failed.
 * When a parent job fails, all its direct and indirect dependent jobs are marked as DEAD
 * and moved to the dead-letter queue.
 *
 * @param failedParentId The ID of the parent job that failed.
 */
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
                // Remove any stale completed entry from workerRecentHistory so the
                // dashboard defaults to WAITING (blue) for blocked downstream jobs
                // rather than showing a stale COMPLETED from a previous run.
                for (java.util.Deque<Job> hist : workerRecentHistory.values()) {
                    hist.removeIf(j -> j.getId().equals(job.getId()));
                }
                cancelChildren(job.getId());
            }
        }
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
    /**
     * Emits every recorded execution as a flat span list: id, status, start, end, worker and the
     * parent edges. This is the same data the dashboard's per-worker history shows, but complete
     * (that view is capped at 10 entries per worker) and on an absolute time axis, so a client can
     * draw a waterfall, derive the critical path, or measure parallelism without new instrumentation.
     * <p>
     * Served on demand rather than folded into the 2s stats poll, since the record map is unbounded.
     *
     * @param dagFilter Only emit jobs whose ID contains this string; empty or {@code null} emits all.
     * @param limit     Maximum spans to emit, most recently started first. Values &lt;= 0 mean no cap.
     * @return A JSON object with a {@code spans} array.
     */
    public String getTimelineJSON(String dagFilter, int limit) {
        java.util.List<TaskExecution> spans = new java.util.ArrayList<>(spanLog);

        if (dagFilter != null && !dagFilter.isEmpty()) {
            final String f = dagFilter;
            spans.removeIf(e -> !e.jobId.contains(f));
        }
        spans.sort((a, b) -> Long.compare(b.startTime, a.startTime));

        boolean truncated = limit > 0 && spans.size() > limit;
        if (truncated) spans = spans.subList(0, limit);

        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"now\": ").append(System.currentTimeMillis()).append(",");
        json.append("\"count\": ").append(spans.size()).append(",");
        json.append("\"truncated\": ").append(truncated).append(",");
        json.append("\"spans\": [");

        for (int i = 0; i < spans.size(); i++) {
            TaskExecution e = spans.get(i);
            String worker = (e.assignedWorker != null) ? String.valueOf(e.assignedWorker.port()) : "unassigned";

            json.append("{")
                .append("\"id\": \"").append(jsonEscape(e.jobId)).append("\",")
                .append("\"attempt\": ").append(e.attempt).append(",")
                .append("\"priority\": ").append(e.priority).append(",")
                .append("\"status\": \"").append(e.status).append("\",")
                .append("\"enqueued_at\": ").append(e.enqueuedAt).append(",")
                .append("\"started_at\": ").append(e.startTime).append(",")
                .append("\"ended_at\": ").append(e.endTime).append(",")
                .append("\"queue_wait_ms\": ").append(e.getQueueWaitMs()).append(",")
                .append("\"duration_ms\": ").append(e.getDuration()).append(",")
                .append("\"worker\": \"").append(jsonEscape(worker)).append("\",")
                .append("\"host\": \"").append(jsonEscape(e.assignedWorker != null ? e.assignedWorker.host() : "")).append("\",")
                .append("\"worker_permanent\": ").append(e.assignedWorker != null && e.assignedWorker.isPermanent()).append(",")
                // Why it ended this way. Already captured by complete()/fail(); truncated because a
                // stack trace or a long stdout tail would bloat every poll.
                .append("\"reason\": \"").append(jsonEscape(shortReason(e.output))).append("\",")
                .append("\"parents\": [");
            for (int pI = 0; pI < e.parents.size(); pI++) {
                json.append("\"").append(jsonEscape(e.parents.get(pI))).append("\"");
                if (pI < e.parents.size() - 1) json.append(",");
            }
            json.append("]}");
            if (i < spans.size() - 1) json.append(",");
        }
        json.append("]}");
        return json.toString();
    }

    /**
     * Emits the sampled control-plane time series: queue composition, dispatch-loop duration,
     * throughput, per-worker heartbeat round-trip, and scaler decisions.
     * <p>
     * Read from in-memory rings, so this is a cheap call and adds no sockets — the dashboard can
     * poll it on the same cadence it already polls stats, or slower.
     *
     * @return A JSON object of named series, each an array of numeric tuples.
     */
    /**
     * The contents of every pre-dispatch holding area, as four lanes.
     * <p>
     * The metrics endpoint reports how many jobs are waiting; this reports <em>which</em>, and why.
     * A job sits in exactly one of these four places before it runs, and until now there was no way
     * to ask where a given job actually was.
     *
     * @param limit Maximum jobs per lane.
     * @return JSON with {@code delayed}, {@code blocked}, {@code ready} and {@code parked} arrays.
     */
    /**
     * Resolves many job statuses in one call, straight from the store.
     * <p>
     * The stats payload carries only {@code workerRecentHistory}, capped at 10 entries per worker,
     * so a graph with more nodes than that can never be fully represented there and the missing
     * ones render as PENDING even though they finished. The store keeps every status, so the
     * visualizer should ask it rather than infer from a rolling window.
     *
     * @param ids Job IDs to resolve.
     * @return JSON object mapping each ID to its status, or {@code UNKNOWN} when the store has none.
     */
    public String getBulkStatusJSON(java.util.List<String> ids) {
        StringBuilder j = new StringBuilder("{");
        int n = 0;
        for (String id : ids) {
            String trimmed = id.trim();
            if (trimmed.isEmpty()) continue;

            // Prefer live in-memory truth for work in flight, fall back to the store for the rest.
            String status;
            if (runningJobs.containsKey(trimmed)) {
                status = "RUNNING";
            } else {
                String fromStore = redisKVGet("job:" + trimmed + ":status");
                if (fromStore == null || fromStore.isEmpty() || "NULL".equals(fromStore)) {
                    TaskExecution rec = executionHistory.get(trimmed);
                    status = (rec != null) ? String.valueOf(rec.status) : "UNKNOWN";
                } else {
                    status = fromStore;
                }
            }
            if (n++ > 0) j.append(",");
            j.append("\"").append(jsonEscape(trimmed)).append("\": \"").append(jsonEscape(status)).append("\"");
        }
        return j.append("}").toString();
    }

    public String getQueueBoardJSON(int limit) {
        long now = System.currentTimeMillis();
        int cap = limit <= 0 ? 50 : limit;
        StringBuilder j = new StringBuilder();
        j.append("{\"now\": ").append(now).append(",");

        // --- DELAYED: timer has not fired ---
        j.append("\"delayed\": [");
        int n = 0;
        for (ScheduledJob sj : waitingRoom) {
            if (n >= cap) break;
            Job job = sj.getJob();
            if (n++ > 0) j.append(",");
            j.append("{\"id\": \"").append(jsonEscape(job.getId()))
             .append("\",\"priority\": ").append(job.getPriority())
             .append(",\"eligible_in_ms\": ").append(Math.max(0, job.getScheduledTime() - now))
             .append("}");
        }
        j.append("],");

        // --- BLOCKED: parents outstanding ---
        j.append("\"blocked\": [");
        n = 0;
        for (Job job : dagWaitingRoom.values()) {
            if (n >= cap) break;
            if (n++ > 0) j.append(",");
            java.util.List<String> miss = job.getUnsatisfiedDeps();
            j.append("{\"id\": \"").append(jsonEscape(job.getId()))
             .append("\",\"priority\": ").append(job.getPriority())
             .append(",\"waiting_ms\": ").append(Math.max(0, now - job.getScheduledTime()))
             .append(",\"waiting_on\": [");
            for (int i = 0; i < miss.size(); i++) {
                j.append("\"").append(jsonEscape(miss.get(i))).append("\"");
                if (i < miss.size() - 1) j.append(",");
            }
            j.append("],\"total_parents\": ")
             .append(job.getDependenciesIds() == null ? 0 : job.getDependenciesIds().size()).append("}");
        }
        j.append("],");

        // --- READY: true dispatch order ---
        // PriorityBlockingQueue.iterator() is explicitly NOT ordered, so a copy is sorted with the
        // queue's own comparator. Iterating directly would show a plausible but wrong "next up".
        java.util.List<Job> ready = new java.util.ArrayList<>(taskQueue);
        java.util.Collections.sort(ready);
        j.append("\"ready\": [");
        n = 0;
        for (Job job : ready) {
            if (n >= cap) break;
            j.append(n > 0 ? "," : "").append("{\"id\": \"").append(jsonEscape(job.getId()))
             .append("\",\"position\": ").append(n + 1)
             .append(",\"priority\": ").append(job.getPriority())
             .append(",\"attempt\": ").append(job.getRetryCount() + 1)
             .append(",\"waiting_ms\": ").append(Math.max(0, now - job.getScheduledTime()))
             .append(",\"requirement\": \"").append(jsonEscape(extractSkillRequirement(job))).append("\"}");
            n++;
        }
        j.append("],");

        // --- PARKED: no worker offers the capability ---
        j.append("\"parked\": [");
        n = 0;
        for (Map.Entry<String, java.util.Deque<Job>> e : parkingLot.entrySet()) {
            for (Job job : e.getValue()) {
                if (n >= cap) break;
                Long since = parkedAt.get(job.getId());
                j.append(n > 0 ? "," : "").append("{\"id\": \"").append(jsonEscape(job.getId()))
                 .append("\",\"capability\": \"").append(jsonEscape(e.getKey()))
                 .append("\",\"priority\": ").append(job.getPriority())
                 .append(",\"parked_ms\": ").append(since == null ? 0 : now - since)
                 .append(",\"reason\": \"").append(jsonEscape(pendingReasons.getOrDefault(job.getId(), ""))).append("\"}");
                n++;
            }
        }
        j.append("],");

        j.append("\"totals\": {\"delayed\": ").append(waitingRoom.size())
         .append(",\"blocked\": ").append(dagWaitingRoom.size())
         .append(",\"ready\": ").append(taskQueue.size())
         .append(",\"parked\": ").append(parkedCount()).append("}}");
        return j.toString();
    }

    public String getMetricsJSON() { return getMetricsJSON("fine"); }

    /**
     * @param res Series resolution: {@code fine} (~10 min at 1s), {@code mid} (~1.7 h at 10s) or
     *            {@code coarse} (~16 h at 100s). Point-in-time blocks are unaffected by this.
     */
    public String getMetricsJSON(String res) {
        StringBuilder j = new StringBuilder();
        j.append("{\"now\": ").append(System.currentTimeMillis()).append(",");
        j.append("\"resolution\": \"").append(jsonEscape(res)).append("\",");
        j.append("\"sample_interval_ms\": ").append("coarse".equals(res) ? 100000 : "mid".equals(res) ? 10000 : 1000).append(",");

        appendSeries(j, "queue", queueSeries, res);      j.append(",");
        appendSeries(j, "dispatch", dispatchSeries, res); j.append(",");
        appendSeries(j, "dispatch_phases", dispatchBreakdown, res); j.append(",");
        // Read latency was collected but never exposed, so the store's cost only showed up as a
        // single write number. Both sit on the dispatch/status path and can diverge.
        appendSeries(j, "store_read_latency", storeReadLatency, res); j.append(",");
        appendSeries(j, "throughput", throughputSeries, res); j.append(",");
        appendSeries(j, "store_latency", storeLatency, res); j.append(",");
        appendSeries(j, "wait_percentiles", waitPercentiles, res); j.append(",");

        appendServices(j); j.append(",");

        j.append("\"worker_load_live\": [");
        int liveKeyCount = 0;
        for (String k : lastLiveWorkerKeys) {
            if (liveKeyCount++ > 0) j.append(",");
            j.append("\"").append(jsonEscape(k)).append("\"");
        }
        j.append("],");

        j.append("\"worker_load\": {");
        int wl = 0;
        for (Map.Entry<String, Series> e : workerLoadSeries.entrySet()) {
            if (wl++ > 0) j.append(",");
            j.append("\"").append(jsonEscape(e.getKey())).append("\": ");
            appendPoints(j, e.getValue(), res);
        }
        j.append("},");

        // Duration buckets across the span log, for a distribution view. Fixed log-ish edges so the
        // shape is comparable between runs rather than rescaling with the data.
        long[] edges = {100, 250, 500, 1000, 2500, 5000, 10000, 30000, Long.MAX_VALUE};
        int[] buckets = new int[edges.length];
        for (TaskExecution te : spanLog) {
            long d = te.getDuration();
            for (int bi = 0; bi < edges.length; bi++) {
                if (d < edges[bi]) { buckets[bi]++; break; }
            }
        }
        j.append("\"duration_histogram\": [");
        for (int bi = 0; bi < buckets.length; bi++) {
            if (bi > 0) j.append(",");
            j.append("{\"lt_ms\": ").append(edges[bi] == Long.MAX_VALUE ? -1 : edges[bi])
             .append(",\"count\": ").append(buckets[bi]).append("}");
        }
        j.append("],");

        j.append("\"heartbeat\": {");
        int w = 0;
        for (Map.Entry<String, Series> e : heartbeatRtt.entrySet()) {
            if (w++ > 0) j.append(",");
            j.append("\"").append(jsonEscape(e.getKey())).append("\": ");
            appendPoints(j, e.getValue(), res);
        }
        j.append("},");

        // ---- dead-letter queue: the clearest "something is broken" signal available ----
        j.append("\"dlq\": {\"depth\": ").append(deadLetterQueue.size()).append(",\"jobs\": [");
        int dq = 0;
        for (Job dj : deadLetterQueue) {
            if (dq >= 20) break;
            if (dq++ > 0) j.append(",");
            TaskExecution rec = executionHistory.get(dj.getId());
            j.append("{\"id\": \"").append(jsonEscape(dj.getId())).append("\"")
             .append(",\"attempts\": ").append(dj.getRetryCount())
             .append(",\"reason\": \"").append(jsonEscape(rec != null ? shortReason(rec.output) : "")).append("\"}");
        }
        j.append("]},");

        // ---- capability supply vs demand: which requirements have nowhere to run ----
        Map<String, Integer> demand = new java.util.TreeMap<>();
        for (Job q : taskQueue) {
            demand.merge(extractSkillRequirement(q), 1, Integer::sum);
        }
        // Parked jobs are demand too — arguably the most important kind, since nothing will satisfy
        // it without operator action.
        Map<String, Integer> parkedByCap = new java.util.TreeMap<>();
        for (Map.Entry<String, java.util.Deque<Job>> e : parkingLot.entrySet()) {
            parkedByCap.put(e.getKey(), e.getValue().size());
            demand.merge(e.getKey(), e.getValue().size(), Integer::sum);
        }
        Map<String, Integer> supply = new java.util.TreeMap<>();
        for (Worker sw : workerRegistry.getWorkers()) {
            for (String cap : sw.capabilities()) supply.merge(cap, 1, Integer::sum);
        }
        java.util.Set<String> caps = new java.util.TreeSet<>(demand.keySet());
        caps.addAll(supply.keySet());
        j.append("\"capability\": [");
        int ci = 0;
        for (String cap : caps) {
            if (ci++ > 0) j.append(",");
            j.append("{\"cap\": \"").append(jsonEscape(cap)).append("\"")
             .append(",\"waiting\": ").append(demand.getOrDefault(cap, 0))
             .append(",\"parked\": ").append(parkedByCap.getOrDefault(cap, 0))
             .append(",\"workers\": ").append(supply.getOrDefault(cap, 0)).append("}");
        }
        j.append("],");

        // ---- why queued jobs are not running ----
        // ---- store health: connection, latency, errors, and its view of cluster state ----
        java.util.List<long[]> sl = storeLatency.snapshot("fine");
        java.util.List<Long> slv = new java.util.ArrayList<>();
        for (long[] pt : sl) slv.add(pt[1]);
        java.util.Collections.sort(slv);
        boolean up = redis.isConnected();

        j.append("\"store\": {")
         .append("\"connected\": ").append(up).append(",")
         .append("\"endpoint\": \"").append(jsonEscape(redis.getHost() + ":" + redis.getPort())).append("\",")
         .append("\"reconnects\": ").append(redis.getReconnectCount()).append(",")
         .append("\"ops\": ").append(storeOps.get()).append(",")
         .append("\"errors\": ").append(storeErrors.get()).append(",")
         .append("\"dropped_writes\": ").append(storeDropped.get()).append(",")
         .append("\"last_error\": \"").append(jsonEscape(storeLastError)).append("\",")
         .append("\"last_error_age_ms\": ").append(storeLastErrorAt == 0 ? -1 : System.currentTimeMillis() - storeLastErrorAt).append(",")
         .append("\"latency_p50\": ").append(pct(slv, 0.50)).append(",")
         .append("\"latency_p99\": ").append(pct(slv, 0.99)).append(",")
         .append("\"latency_max\": ").append(slv.isEmpty() ? 0 : slv.get(slv.size() - 1)).append(",");

        java.util.List<Long> srv = new java.util.ArrayList<>();
        for (long[] pt : storeReadLatency.snapshot("fine")) srv.add(pt[1]);
        java.util.Collections.sort(srv);
        j.append("\"reads\": ").append(storeReads.get()).append(",")
         .append("\"read_p50\": ").append(pct(srv, 0.50)).append(",")
         .append("\"read_p99\": ").append(pct(srv, 0.99)).append(",")
         .append("\"last_write_age_ms\": ").append(storeLastWriteOk == 0 ? -1 : System.currentTimeMillis() - storeLastWriteOk).append(",");

        // The store's own view of cluster state — should agree with the Master's in-memory view.
        int lw = 0, ls = 0, aj = 0;
        if (up) {
            try {
                java.util.Set<String> a = safeRedisSMembers("system:live_workers");
                java.util.Set<String> b = safeRedisSMembers("system:live_services");
                java.util.Set<String> cset = safeRedisSMembers("system:active_jobs");
                lw = a == null ? 0 : a.size();
                ls = b == null ? 0 : b.size();
                aj = cset == null ? 0 : cset.size();
            } catch (Exception ignored) { /* health must never throw */ }
        }
        j.append("\"live_workers\": ").append(lw).append(",")
         .append("\"live_services\": ").append(ls).append(",")
         .append("\"active_jobs\": ").append(aj).append(",")
         .append("\"master_workers\": ").append(workerRegistry.getWorkers().size()).append(",")
         // If the Master restarted right now, this is what recoverState() would pick up.
         .append("\"recoverable_jobs\": ").append(aj).append("},");

        j.append("\"parked\": {\"total\": ").append(parkedCount())
         .append(",\"deadline_ms\": ").append(PARK_DEADLINE_MS).append(",\"by_capability\": [");
        int pk = 0;
        for (Map.Entry<String, java.util.Deque<Job>> e : parkingLot.entrySet()) {
            if (pk++ > 0) j.append(",");
            long oldest = 0;
            for (Job pj : e.getValue()) {
                Long t = parkedAt.get(pj.getId());
                if (t != null) oldest = (oldest == 0) ? t : Math.min(oldest, t);
            }
            j.append("{\"cap\": \"").append(jsonEscape(e.getKey()))
             .append("\",\"count\": ").append(e.getValue().size())
             .append(",\"oldest_wait_ms\": ").append(oldest == 0 ? 0 : System.currentTimeMillis() - oldest)
             .append("}");
        }
        j.append("]},");

        // Capped for payload size, but the cap was silent AND the order was arbitrary — a
        // ConcurrentHashMap iteration gave an unpredictable 25 of N, so a reader could not tell
        // the list was partial, and the same job could appear or vanish between refreshes.
        // Sort by how long each job has been stuck: the ones waiting longest are the ones worth
        // reading, and the ordering is now stable between polls.
        java.util.List<Map.Entry<String, String>> prList =
                new java.util.ArrayList<>(pendingReasons.entrySet());
        prList.sort((a, b) -> {
            Long pa = parkedAt.get(a.getKey()), pb = parkedAt.get(b.getKey());
            long va = (pa == null) ? Long.MAX_VALUE : pa;
            long vb = (pb == null) ? Long.MAX_VALUE : pb;
            int byAge = Long.compare(va, vb);                 // oldest first
            return byAge != 0 ? byAge : a.getKey().compareTo(b.getKey());
        });
        j.append("\"pending_reasons_total\": ").append(prList.size()).append(",");
        j.append("\"pending_reasons\": [");
        int pr = 0;
        for (Map.Entry<String, String> e : prList) {
            if (pr >= PENDING_REASON_CAP) break;
            if (pr++ > 0) j.append(",");
            j.append("{\"id\": \"").append(jsonEscape(e.getKey()))
             .append("\",\"reason\": \"").append(jsonEscape(e.getValue())).append("\"}");
        }
        j.append("],");

        // ---- latency percentiles + retry distribution, derived from the span log ----
        java.util.List<Long> waits = new java.util.ArrayList<>();
        java.util.Map<Integer, Integer> attempts = new java.util.TreeMap<>();
        int retried = 0, total = 0;
        for (TaskExecution e : spanLog) {
            if (e.enqueuedAt > 0) waits.add(e.getQueueWaitMs());
            attempts.merge(e.attempt, 1, Integer::sum);
            total++;
            if (e.attempt > 1) retried++;
        }
        java.util.Collections.sort(waits);
        j.append("\"queue_wait\": {")
         .append("\"p50\": ").append(pct(waits, 0.50)).append(",")
         .append("\"p95\": ").append(pct(waits, 0.95)).append(",")
         .append("\"p99\": ").append(pct(waits, 0.99)).append(",")
         .append("\"max\": ").append(waits.isEmpty() ? 0 : waits.get(waits.size() - 1)).append(",")
         .append("\"n\": ").append(waits.size()).append("},");

        j.append("\"retries\": {\"dispatches\": ").append(total)
         .append(",\"rate\": ").append(total == 0 ? 0 : Math.round(1000.0 * retried / total) / 1000.0)
         .append(",\"histogram\": [");
        int hi = 0;
        for (Map.Entry<Integer, Integer> e : attempts.entrySet()) {
            if (hi++ > 0) j.append(",");
            j.append("{\"attempt\": ").append(e.getKey()).append(",\"count\": ").append(e.getValue()).append("}");
        }
        j.append("]},");

        int ephemNow = 0;
        for (Worker ew : workerRegistry.getWorkers()) if (!ew.isPermanent()) ephemNow++;
        j.append("\"scaling\": {\"scale_ups\": ").append(scaleUpCount.get())
         .append(",\"descales\": ").append(descaleCount.get())
         .append(",\"ephemeral_now\": ").append(ephemNow)
         .append(",\"peak_workers\": ").append(peakWorkers)
         .append(",\"max_workers\": ").append(MAX_WORKERS).append("},");

        j.append("\"scaler_events\": [");
        int k = 0;
        for (String ev : scalerEvents) {
            String[] parts = ev.split("\\|", 3);
            if (parts.length < 2) continue;
            if (k++ > 0) j.append(",");
            j.append("{\"ts\": ").append(parts[0])
             .append(",\"type\": \"").append(jsonEscape(parts[1])).append("\"")
             .append(",\"detail\": \"").append(jsonEscape(parts.length > 2 ? parts[2] : "")).append("\"}");
        }
        j.append("]}");
        return j.toString();
    }

    /**
     * Nearest-rank percentile over a pre-sorted list.
     *
     * @param sorted Ascending values; may be empty.
     * @param q Quantile in [0,1].
     * @return The value at that quantile, or 0 for an empty list.
     */
    private static long pct(java.util.List<Long> sorted, double q) {
        if (sorted.isEmpty()) return 0;
        int idx = (int) Math.ceil(q * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(idx, sorted.size() - 1)));
    }

    /**
     * Splits one dispatch iteration into its five phases and records them.
     *
     * @param iterStart when the job left the queue.
     * @param tRouted after the capability lookup returned candidate workers.
     * @param tSelected after a worker was chosen.
     * @param tRecorded after the span and in-memory bookkeeping were written.
     * @param tStored after the status/worker writes to the store returned.
     */
    private void recordDispatchPhases(long iterStart, long tRouted, long tSelected,
                                      long tRecorded, long tStored) {
        long now = System.currentTimeMillis();
        dispatchBreakdown.add(now,
                Math.max(0, tRouted - iterStart),     // route: capability lookup
                Math.max(0, tSelected - tRouted),     // select: worker choice
                Math.max(0, tRecorded - tSelected),   // record: span + bookkeeping
                Math.max(0, tStored - tRecorded),     // store: writes on the dispatch path
                Math.max(0, now - tStored));          // send: hand-off to the worker
    }

    /**
     * The live services, with the address a caller would actually dial and how long each has been
     * up. Long-running work is the half of the workload a job-centric view cannot describe: a
     * service has no duration, so it never appears in a throughput or duration chart.
     *
     * @param j the metrics payload being assembled.
     */
    private void appendServices(StringBuilder j) {
        j.append("\"services\": [");
        long now = System.currentTimeMillis();
        int n = 0;
        for (Map.Entry<String, Worker> e : liveServiceMap.entrySet()) {
            String id = e.getKey();
            // An autoscaler-spawned worker is bootstrapped through the same deploy path, so it
            // sits in liveServiceMap under a "WRK-<port>-<uuid>" id until it is promoted to a
            // peer. It is a node, not a user service: it belongs in Topology, and its port
            // speaks the Titan protocol rather than anything a caller would dial.
            if (id.contains("WRK-")) continue;
            Worker w = e.getValue();
            String info = serviceInfo.get(id);
            String host = (w != null) ? w.host() : "";
            int port = 0;
            long since = 0;
            if (info != null) {
                // "host:port:since" — split from the right, because a host can contain colons.
                int c2 = info.lastIndexOf(':');
                int c1 = (c2 > 0) ? info.lastIndexOf(':', c2 - 1) : -1;
                if (c1 > 0) {
                    host = info.substring(0, c1);
                    try {
                        port = Integer.parseInt(info.substring(c1 + 1, c2));
                        since = Long.parseLong(info.substring(c2 + 1));
                    } catch (NumberFormatException ignored) { /* leave zeroed */ }
                }
            }
            TaskExecution rec = executionHistory.get(id);
            if (n++ > 0) j.append(",");
            j.append("{\"id\":\"").append(jsonEscape(id)).append("\"")
             .append(",\"host\":\"").append(jsonEscape(host)).append("\"")
             .append(",\"port\":").append(port)
             .append(",\"worker\":\"")
             .append(jsonEscape(w != null ? w.host() + ":" + w.port() : "unassigned")).append("\"")
             .append(",\"worker_permanent\":").append(w != null && w.isPermanent())
             .append(",\"since\":").append(since)
             .append(",\"uptime_ms\":").append(since > 0 ? Math.max(0, now - since) : -1)
             // Deploy attempts, not process restarts: a crash-looping service is restarted by the
             // worker, which never tells the Master, so claiming a restart count here would lie.
             .append(",\"deploy_attempts\":").append(rec != null ? rec.attempt : 1)
             .append(",\"status\":\"").append(rec != null ? rec.status : "RUNNING").append("\"")
             .append("}");
        }
        j.append("]");
    }

    private void appendSeries(StringBuilder j, String name, Series s, String res) {
        j.append("\"").append(name).append("\": ");
        appendPoints(j, s, res);
    }

    private void appendPoints(StringBuilder j, Series s, String res) {
        java.util.List<long[]> pts = s.snapshot(res);
        j.append("[");
        for (int i = 0; i < pts.size(); i++) {
            long[] p = pts.get(i);
            j.append("[");
            for (int v = 0; v < p.length; v++) {
                j.append(p[v]);
                if (v < p.length - 1) j.append(",");
            }
            j.append("]");
            if (i < pts.size() - 1) j.append(",");
        }
        j.append("]");
    }

    /**
     * Condenses a job's recorded output into a one-line reason suitable for a tooltip.
     *
     * @param output The raw output or error text; may be {@code null}.
     * @return A single line of at most 220 characters, or an empty string.
     */
    private static String shortReason(String output) {
        if (output == null || output.isBlank()) return "";
        String text = output.trim();

        // Worker callbacks arrive as "STATUS|exitCode|stdout". Surface the exit code and the most
        // informative line rather than the envelope: for a traceback the LAST line names the error,
        // the first is always the useless "Traceback (most recent call last):".
        String prefix = "";
        String[] parts = text.split("\\|", 3);
        if (parts.length == 3 && parts[1].matches("-?\\d+")) {
            if (!"0".equals(parts[1])) prefix = "exit " + parts[1] + ": ";
            text = parts[2].trim();
        }

        String[] lines = text.split("\\R");
        String pick = "";
        for (int i = lines.length - 1; i >= 0; i--) {
            String candidate = lines[i].trim();
            if (candidate.isEmpty()) continue;
            if (candidate.startsWith("Traceback") || candidate.startsWith("File \"")) continue;
            pick = candidate;
            break;
        }
        if (pick.isEmpty() && lines.length > 0) pick = lines[0].trim();

        String result = prefix + pick;
        return result.length() > 220 ? result.substring(0, 217) + "..." : result;
    }

    /** Minimal JSON string escaping for IDs that may contain quotes or backslashes. */
    private static String jsonEscape(String v) {
        if (v == null) return "";
        return v.replace("\\", "\\\\").replace("\"", "\\\"");
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
            json.append("\"host\": \"").append(jsonEscape(w.host())).append("\",");
            // Lets the UI distinguish a node you started from one the scaler spawned and may reclaim.
            json.append("\"permanent\": ").append(w.isPermanent()).append(",");
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
                            j.getId(), j.getStatus(), duration, completedAt));
                    if (hCount < history.size() - 1) json.append(",");
                    hCount++;
                }
            }
            json.append("],");

            // Host vitals sit next to slot occupancy deliberately: read together they say whether
            // a full pool is actually working or merely holding jobs.
            json.append("\"host_cpu_pct\": ").append(w.hostCpuPct()).append(",");
            json.append("\"host_mem_pct\": ").append(w.hostMemPct()).append(",");
            json.append("\"host_load_x100\": ").append(w.hostLoadX100()).append(",");

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

    /**
 * Retrieves the current status of a job based on its ID.
 *
 * @param id The ID of the job.
 * @return The {@link Job.Status} of the job, or {@link Job.Status#PENDING} if the job is not found in execution history.
 */
    public Job.Status getJobStatus(String id) {
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
            dispatchExecutor.shutdownNow();
        }
    }
}

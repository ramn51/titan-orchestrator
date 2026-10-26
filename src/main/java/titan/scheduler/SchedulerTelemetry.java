package titan.scheduler;

import titan.TitanConfig;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/** Fixed-size scheduler samples and events for the Cluster view. */
final class SchedulerTelemetry {
    private final int limit = Math.max(10, TitanConfig.getInt("titan.observability.metric.samples", 900));
    private final Deque<String> samples = new ArrayDeque<>();
    private final Deque<String> samples10 = new ArrayDeque<>();
    private final Deque<String> samples100 = new ArrayDeque<>();
    private long sampleCount;
    private final Deque<String> events = new ArrayDeque<>();
    private final Deque<String> dispatchPhases = new ArrayDeque<>();
    private final Deque<String> storeOperations = new ArrayDeque<>();
    private final Deque<String> workerPhases = new ArrayDeque<>();
    private final AtomicLong dispatched = new AtomicLong(), completed = new AtomicLong(), failed = new AtomicLong();
    private final AtomicLong heartbeats = new AtomicLong(), missedHeartbeats = new AtomicLong();
    private final AtomicLong storeWriteFailures = new AtomicLong();

    void dispatched() { dispatched.incrementAndGet(); }
    void completed() { completed.incrementAndGet(); }
    void failed() { failed.incrementAndGet(); }
    void heartbeat(boolean ok) { if (ok) heartbeats.incrementAndGet(); else missedHeartbeats.incrementAndGet(); }
    void storeWrite(long durationNanos, boolean succeeded) {
        if (!succeeded) storeWriteFailures.incrementAndGet();
        storeOperation("write", durationNanos);
    }
    void storeRead(long durationNanos) { storeOperation("read", durationNanos); }
    private synchronized void storeOperation(String kind, long durationNanos) {
        storeOperations.addLast("{\"at\":" + System.currentTimeMillis() + ",\"kind\":"
                + ExecutionTimeline.quoted(kind) + ",\"ms\":" + durationNanos / 1_000_000.0 + "}");
        while (storeOperations.size() > limit) storeOperations.removeFirst();
    }
    synchronized void dispatchPhase(String jobId, long routeNs, long selectNs, long recordNs, long storeNs, long sendNs) {
        dispatchPhases.addLast("{\"at\":" + System.currentTimeMillis()
                + ",\"job_id\":" + ExecutionTimeline.quoted(jobId)
                + ",\"route\":" + routeNs / 1_000_000.0
                + ",\"select\":" + selectNs / 1_000_000.0
                + ",\"record\":" + recordNs / 1_000_000.0
                + ",\"store\":" + storeNs / 1_000_000.0
                + ",\"send\":" + sendNs / 1_000_000.0 + "}");
        while (dispatchPhases.size() > limit) dispatchPhases.removeFirst();
    }

    synchronized void workerPhase(String jobId, Map<String, Long> timing,
                                  long callbackTransitMs, long callbackHandleNs,
                                  long terminalPersistenceNs) {
        workerPhases.addLast("{\"at\":" + System.currentTimeMillis()
                + ",\"job_id\":" + ExecutionTimeline.quoted(jobId)
                + ",\"worker_queue\":" + timing.getOrDefault("worker_queue_ns", 0L) / 1_000_000.0
                + ",\"preprocess\":" + timing.getOrDefault("preprocess_ns", 0L) / 1_000_000.0
                + ",\"process_startup\":" + timing.getOrDefault("process_startup_ns", 0L) / 1_000_000.0
                + ",\"script\":" + timing.getOrDefault("script_ns", 0L) / 1_000_000.0
                + ",\"callback_transit\":" + Math.max(0L, callbackTransitMs)
                + ",\"callback_handle\":" + callbackHandleNs / 1_000_000.0
                + ",\"terminal_persistence\":" + terminalPersistenceNs / 1_000_000.0 + "}");
        while (workerPhases.size() > limit) workerPhases.removeFirst();
    }

    synchronized void event(String type, String details) {
        events.addLast("{\"at\":" + System.currentTimeMillis() + ",\"type\":"
                + ExecutionTimeline.quoted(type) + ",\"details\":" + ExecutionTimeline.quoted(details) + "}");
        while (events.size() > limit) events.removeFirst();
    }

    synchronized void sample(int ready, int dependencyWait, int capabilityWait, int delayed,
                             long oldestReadyMs, long oldestParkedMs,
                             int running, Collection<Worker> workers, boolean storeHealthy,
                             long droppedSpans, int pendingSpanWrites) {
        StringBuilder out = new StringBuilder("{\"at\":").append(System.currentTimeMillis())
                .append(",\"queue_depth\":").append(ready)
                .append(",\"dependency_wait\":").append(dependencyWait)
                .append(",\"capability_wait\":").append(capabilityWait)
                .append(",\"delayed\":").append(delayed)
                .append(",\"oldest_ready_ms\":").append(oldestReadyMs)
                .append(",\"oldest_parked_ms\":").append(oldestParkedMs)
                .append(",\"running\":").append(running)
                .append(",\"dispatched\":").append(dispatched.get())
                .append(",\"completed\":").append(completed.get())
                .append(",\"failed\":").append(failed.get())
                .append(",\"heartbeats\":").append(heartbeats.get())
                .append(",\"missed_heartbeats\":").append(missedHeartbeats.get())
                .append(",\"store_healthy\":").append(storeHealthy)
                .append(",\"dropped_span_writes\":").append(droppedSpans)
                .append(",\"pending_span_writes\":").append(pendingSpanWrites)
                .append(",\"store_dropped_writes\":").append(storeWriteFailures.get())
                .append(",\"workers\":[");
        int i = 0;
        for (Worker worker : workers) {
            if (i++ > 0) out.append(',');
            out.append("{\"id\":").append(ExecutionTimeline.quoted(worker.host() + ":" + worker.port()))
                    .append(",\"load\":").append(worker.getCurrentLoad())
                    .append(",\"capacity\":").append(worker.getMaxCap())
                    .append(",\"cpu_percent\":").append(worker.hostCpuPercent())
                    .append(",\"memory_used_bytes\":").append(worker.hostMemoryUsedBytes())
                    .append(",\"memory_total_bytes\":").append(worker.hostMemoryTotalBytes())
                    .append(",\"host_load_average\":").append(worker.hostLoadAverage())
                    .append(",\"heartbeat_rtt_ms\":").append(worker.heartbeatRttMs())
                    .append(",\"permanent\":").append(worker.isPermanent())
                    .append(",\"capabilities\":").append(ExecutionTimeline.quoted(String.join(",", worker.capabilities())))
                    .append(",\"last_seen\":").append(worker.lastSeen()).append('}');
        }
        String point = out.append("]}").toString();
        samples.addLast(point);
        while (samples.size() > limit) samples.removeFirst();
        sampleCount++;
        if (sampleCount % 10 == 0) {
            samples10.addLast(point);
            while (samples10.size() > limit) samples10.removeFirst();
        }
        if (sampleCount % 100 == 0) {
            samples100.addLast(point);
            while (samples100.size() > limit) samples100.removeFirst();
        }
    }

    synchronized String json(long since, int count) {
        int bounded = Math.max(1, Math.min(count, limit));
        long age = System.currentTimeMillis() - since;
        Deque<String> resolution = since == 0 || age > limit * 10_000L ? samples100
                : age > limit * 1_000L ? samples10 : samples;
        int seconds = resolution == samples100 ? 100 : resolution == samples10 ? 10 : 1;
        return "{\"resolution_seconds\":" + seconds + ",\"samples\":" + recent(resolution, since, bounded) + ",\"events\":"
                + recent(events, since, bounded) + ",\"dispatch_phases\":"
                + recent(dispatchPhases, since, bounded) + ",\"store_operations\":"
                + recent(storeOperations, since, bounded) + ",\"worker_phases\":"
                + recent(workerPhases, since, bounded) + "}";
    }
    private static String recent(Deque<String> source, long since, int limit) {
        ArrayList<String> result = new ArrayList<>();
        for (Iterator<String> it = source.descendingIterator(); it.hasNext() && result.size() < limit;) {
            String item = it.next();
            int at = item.indexOf(":") + 1, end = item.indexOf(',', at);
            if (Long.parseLong(item.substring(at, end)) >= since) result.add(item);
        }
        return "[" + String.join(",", result) + "]";
    }
}

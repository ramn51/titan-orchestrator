package titan.scheduler;

import titan.TitanConfig;
import java.io.IOException;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/** Bounded live execution attempts, journaled asynchronously by UTC day. */
final class ExecutionTimeline implements AutoCloseable {
    private final Path directory;
    private Path singleJournal;
    private final int liveLimit, retentionDays;
    private final Deque<Span> spans = new ArrayDeque<>();
    private final Map<String, Span> active = new HashMap<>();
    private final BlockingQueue<String> journalQueue;
    private final AtomicLong droppedWrites = new AtomicLong();
    private final Thread writer;
    private volatile boolean writing = true;

    ExecutionTimeline() {
        this(Path.of(TitanConfig.get("titan.observability.directory", "telemetry")),
                TitanConfig.getInt("titan.observability.live.spans", 5000),
                TitanConfig.getInt("titan.observability.retention.days", 14),
                TitanConfig.getInt("titan.observability.write.queue", 8192));
    }
    ExecutionTimeline(Path journal) {
        this(journal.getParent() == null ? Path.of(".") : journal.getParent(), 1000, 14, 1024);
        this.singleJournal = journal;
    }
    ExecutionTimeline(Path directory, int liveLimit, int retentionDays, int writeQueueLimit) {
        this.directory = directory;
        this.liveLimit = Math.max(1, liveLimit);
        this.retentionDays = Math.max(1, retentionDays);
        this.journalQueue = new ArrayBlockingQueue<>(Math.max(1, writeQueueLimit));
        this.writer = new Thread(this::writeLoop, "titan-span-journal");
        this.writer.setDaemon(true);
        this.writer.start();
    }

    synchronized void start(Job job, Worker worker, long eligibleAt) {
        long now = System.currentTimeMillis();
        Span span = new Span(job.getId(), worker.host() + ":" + worker.port(),
                Math.min(eligibleAt, now), now, job.getRetryCount() + 1,
                job.getPriority(), job.getDependenciesIds());
        active.put(job.getId(), span);
        spans.addLast(span);
        while (spans.size() > liveLimit) spans.removeFirst();
    }
    synchronized void finish(String jobId, String status, String reason) {
        Span span = active.remove(jobId);
        if (span == null) return;
        span.endedAt = System.currentTimeMillis();
        span.status = status;
        span.reason = reason == null ? "" : reason.substring(0, Math.min(8000, reason.length()));
        if (!journalQueue.offer(span.json())) droppedWrites.incrementAndGet();
    }
    /** Removes a provisional dispatch that the worker declined before execution. */
    synchronized void discard(String jobId) {
        Span span = active.remove(jobId);
        if (span != null) spans.remove(span);
    }
    synchronized String json() { return json(0, liveLimit); }
    synchronized String json(long since, int limit) {
        int bounded = Math.max(1, Math.min(limit, liveLimit));
        StringBuilder out = new StringBuilder("[");
        int written = 0;
        for (Iterator<Span> it = spans.descendingIterator(); it.hasNext() && written < bounded;) {
            Span span = it.next();
            if (span.startedAt < since) continue;
            if (written++ > 0) out.append(',');
            out.append(span.json());
        }
        return out.append(']').toString();
    }
    String history(LocalDate day, int limit) {
        if (day.isBefore(LocalDate.now(ZoneOffset.UTC).minusDays(retentionDays))) return "[]";
        Path path = directory.resolve("spans-" + day + ".jsonl");
        if (!Files.isRegularFile(path)) return "[]";
        int bounded = Math.max(1, Math.min(limit, 5000));
        Deque<String> tail = new ArrayDeque<>(bounded);
        try (Stream<String> lines = Files.lines(path)) {
            lines.forEach(line -> { if (tail.size() == bounded) tail.removeFirst(); tail.addLast(line); });
        } catch (IOException e) { return "[]"; }
        return "[" + String.join(",", tail) + "]";
    }
    long droppedWrites() { return droppedWrites.get(); }
    int pendingWrites() { return journalQueue.size(); }
    private void writeLoop() {
        LocalDate cleaned = null;
        while (writing || !journalQueue.isEmpty()) {
            try {
                String line = journalQueue.poll(250, TimeUnit.MILLISECONDS);
                if (line == null) continue;
                LocalDate today = LocalDate.now(ZoneOffset.UTC);
                Files.createDirectories(directory);
                Files.writeString(singleJournal == null ? directory.resolve("spans-" + today + ".jsonl") : singleJournal,
                        line + System.lineSeparator(),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                if (!today.equals(cleaned)) { removeExpired(today); cleaned = today; }
            } catch (InterruptedException e) {
                if (!writing) break;
            } catch (IOException e) {
                droppedWrites.incrementAndGet();
                System.err.println("[TIMELINE] Could not write span: " + e.getMessage());
            }
        }
    }
    private void removeExpired(LocalDate today) throws IOException {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(directory, "spans-*.jsonl")) {
            for (Path path : files) {
                String name = path.getFileName().toString();
                try {
                    LocalDate date = LocalDate.parse(name.substring(6, 16));
                    if (date.isBefore(today.minusDays(retentionDays))) Files.deleteIfExists(path);
                } catch (RuntimeException ignored) { }
            }
        }
    }
    @Override public void close() {
        writing = false;
        try { writer.join(3000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
    static String quoted(String value) {
        StringBuilder out = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            if (c == '\\' || c == '"') out.append('\\').append(c);
            else if (c == '\n') out.append("\\n");
            else if (c == '\r') out.append("\\r");
            else if (c == '\t') out.append("\\t");
            else if (c < 32) out.append(' ');
            else out.append(c);
        }
        return out.append('"').toString();
    }
    private static final class Span {
        final String jobId, worker;
        final long queuedAt, startedAt;
        final int attempt, priority;
        final List<String> parents;
        long endedAt;
        String status = "RUNNING", reason = "";
        Span(String jobId, String worker, long queuedAt, long startedAt,
             int attempt, int priority, List<String> parents) {
            this.jobId = jobId; this.worker = worker; this.queuedAt = queuedAt;
            this.startedAt = startedAt; this.attempt = attempt; this.priority = priority;
            this.parents = List.copyOf(parents);
        }
        String json() {
            StringBuilder out = new StringBuilder("{\"job_id\":").append(quoted(jobId))
                    .append(",\"worker\":").append(quoted(worker))
                    .append(",\"queued_at\":").append(queuedAt)
                    .append(",\"eligible_at\":").append(queuedAt)
                    .append(",\"started_at\":").append(startedAt)
                    .append(",\"ended_at\":").append(endedAt)
                    .append(",\"attempt\":").append(attempt)
                    .append(",\"priority\":").append(priority)
                    .append(",\"status\":").append(quoted(status))
                    .append(",\"reason\":").append(quoted(reason))
                    .append(",\"parents\":[");
            for (int i = 0; i < parents.size(); i++) {
                if (i > 0) out.append(',');
                out.append(quoted(parents.get(i)));
            }
            return out.append("]}").toString();
        }
    }
}

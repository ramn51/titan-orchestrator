package titan.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Bounded, nonblocking TCP readiness checks for newly started services. */
final class ServiceReadinessMonitor implements AutoCloseable {
    interface Probe { boolean reachable(String host, int port); }

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2, task -> {
        Thread thread = new Thread(task, "titan-service-readiness");
        thread.setDaemon(true);
        return thread;
    });
    private final Map<String, Boolean> pending = new ConcurrentHashMap<>();
    private final Probe probe;
    private final int attempts;
    private final long delayMs;

    ServiceReadinessMonitor() { this(ServiceReadinessMonitor::tcpReachable, 10, 2000); }

    ServiceReadinessMonitor(Probe probe, int attempts, long delayMs) {
        this.probe = probe;
        this.attempts = attempts;
        this.delayMs = delayMs;
    }

    void watch(String id, String host, int port, Consumer<Boolean> completion) {
        if (pending.putIfAbsent(id, Boolean.TRUE) != null)
            throw new IllegalStateException("Readiness check already active: " + id);
        schedule(id, host, port, 1, completion, 0);
    }

    void cancel(String id) { pending.remove(id); }

    private void schedule(String id, String host, int port, int attempt,
                          Consumer<Boolean> completion, long delay) {
        executor.schedule(() -> {
            if (!pending.containsKey(id)) return;
            boolean ready;
            try { ready = probe.reachable(host, port); }
            catch (RuntimeException error) {
                System.err.println("[DEPLOY] Readiness probe failed for " + id + ": " + error);
                ready = false;
            }
            if (ready || attempt >= attempts) {
                if (pending.remove(id) != null) completion.accept(ready);
            } else {
                schedule(id, host, port, attempt + 1, completion, delayMs);
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    private static boolean tcpReachable(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 300);
            return true;
        } catch (IOException error) {
            return false;
        }
    }

    @Override public void close() {
        pending.clear();
        executor.shutdownNow();
    }
}

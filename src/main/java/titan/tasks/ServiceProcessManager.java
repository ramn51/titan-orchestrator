package titan.tasks;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Owns a worker's long-lived child processes and their bounded crash recovery. */
public final class ServiceProcessManager {
    @FunctionalInterface
    public interface Launcher { Process launch() throws IOException; }

    interface Registry {
        void register(String id, long pid);
        void unregister(String id);
    }

    private static final int DEFAULT_MAX_RESTARTS = 3;
    private static final long DEFAULT_DELAY_MS = 1000;
    private final Map<String, Service> services = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private final Registry registry;
    private final Consumer<String> notifyStopped;
    private final int maxRestarts;
    private final long firstDelayMs;

    public ServiceProcessManager(Consumer<String> notifyStopped) {
        this(notifyStopped, new Registry() {
            public void register(String id, long pid) { ProcessRegistry.register(id, pid); }
            public void unregister(String id) { ProcessRegistry.unregister(id); }
        }, DEFAULT_MAX_RESTARTS, DEFAULT_DELAY_MS);
    }

    ServiceProcessManager(Consumer<String> notifyStopped, Registry registry,
                          int maxRestarts, long firstDelayMs) {
        this.notifyStopped = notifyStopped;
        this.registry = registry;
        this.maxRestarts = maxRestarts;
        this.firstDelayMs = firstDelayMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "titan-service-restarts");
            thread.setDaemon(true);
            return thread;
        });
    }

    public String start(String id, Launcher launcher, Consumer<Process> streamOutput) {
        Service service = new Service(id, launcher, streamOutput);
        if (services.putIfAbsent(id, service) != null) return "SERVICE_ALREADY_RUNNING: " + id;
        synchronized (service) {
            try {
                Process process = launch(service);
                return "DEPLOYED_SUCCESS | ID: " + id + " | PID: " + process.pid();
            } catch (IOException error) {
                services.remove(id, service);
                return "LAUNCH_ERROR: " + error.getMessage();
            }
        }
    }

    public String stop(String id) {
        Service service = services.get(id);
        if (service == null) return "UNKNOWN_SERVICE: " + id;
        boolean notify = false;
        synchronized (service) {
            service.intentionalStop = true;
            if (service.pendingRestart != null) service.pendingRestart.cancel(false);
            if (service.process == null) {
                notify = finish(service);
            } else {
                service.process.descendants().forEach(ProcessHandle::destroyForcibly);
                service.process.destroyForcibly();
            }
        }
        if (notify) notifyStopped.accept(id);
        return "STOPPED: " + id;
    }

    private Process launch(Service service) throws IOException {
        Process process = service.launcher.launch();
        try {
            registry.register(service.id, process.pid());
        } catch (RuntimeException error) {
            process.descendants().forEach(ProcessHandle::destroyForcibly);
            process.destroyForcibly();
            throw new IOException("Could not journal service process", error);
        }
        service.process = process;
        process.onExit().whenComplete((ignored, error) -> exited(service, process, error));
        service.streamOutput.accept(process);
        return process;
    }

    private void exited(Service service, Process process, Throwable error) {
        boolean notify = false;
        synchronized (service) {
            if (service.process != process) return;
            registry.unregister(service.id);
            service.process = null;
            if (service.intentionalStop || service.restarts >= maxRestarts) {
                if (!service.intentionalStop) {
                    System.err.println("[SERVICE] " + service.id + " exited; restart limit reached ("
                            + maxRestarts + ")." + (error == null ? "" : " " + error));
                }
                notify = finish(service);
            } else {
                scheduleRestart(service);
            }
        }
        if (notify) notifyStopped.accept(service.id);
    }

    private void scheduleRestart(Service service) {
        int attempt = ++service.restarts;
        long delay = firstDelayMs * (1L << Math.min(attempt - 1, 10));
        System.err.println("[SERVICE] " + service.id + " exited unexpectedly; restart "
                + attempt + "/" + maxRestarts + " in " + delay + "ms.");
        service.pendingRestart = scheduler.schedule(() -> restart(service), delay, TimeUnit.MILLISECONDS);
    }

    private void restart(Service service) {
        boolean notify = false;
        synchronized (service) {
            service.pendingRestart = null;
            if (service.intentionalStop || services.get(service.id) != service) return;
            try {
                launch(service);
            } catch (IOException error) {
                System.err.println("[SERVICE] Could not restart " + service.id + ": " + error);
                if (service.restarts >= maxRestarts) notify = finish(service);
                else scheduleRestart(service);
            }
        }
        if (notify) notifyStopped.accept(service.id);
    }

    private boolean finish(Service service) {
        if (service.finished) return false;
        service.finished = true;
        services.remove(service.id, service);
        return true;
    }

    private static final class Service {
        final String id;
        final Launcher launcher;
        final Consumer<Process> streamOutput;
        Process process;
        ScheduledFuture<?> pendingRestart;
        int restarts;
        boolean intentionalStop;
        boolean finished;

        Service(String id, Launcher launcher, Consumer<Process> streamOutput) {
            this.id = id;
            this.launcher = launcher;
            this.streamOutput = streamOutput;
        }
    }
}

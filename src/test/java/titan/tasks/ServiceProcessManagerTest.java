package titan.tasks;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class ServiceProcessManagerTest {
    private static final class FakeProcess extends Process {
        private final long id;
        private final CompletableFuture<Process> exit = new CompletableFuture<>();
        private volatile boolean alive = true;

        FakeProcess(long id) { this.id = id; }
        void finish() { alive = false; exit.complete(this); }
        @Override public OutputStream getOutputStream() { return new ByteArrayOutputStream(); }
        @Override public InputStream getInputStream() { return new ByteArrayInputStream(new byte[0]); }
        @Override public InputStream getErrorStream() { return new ByteArrayInputStream(new byte[0]); }
        @Override public int waitFor() throws InterruptedException {
            try { exit.get(); return 1; } catch (java.util.concurrent.ExecutionException e) { throw new AssertionError(e); }
        }
        @Override public boolean waitFor(long timeout, TimeUnit unit) throws InterruptedException {
            try { exit.get(timeout, unit); return true; }
            catch (java.util.concurrent.TimeoutException e) { return false; }
            catch (java.util.concurrent.ExecutionException e) { throw new AssertionError(e); }
        }
        @Override public int exitValue() {
            if (alive) throw new IllegalThreadStateException();
            return 1;
        }
        @Override public void destroy() { finish(); }
        @Override public Process destroyForcibly() { finish(); return this; }
        @Override public boolean isAlive() { return alive; }
        @Override public long pid() { return id; }
        @Override public Stream<ProcessHandle> descendants() { return Stream.empty(); }
        @Override public CompletableFuture<Process> onExit() { return exit; }
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(5);
        assertTrue(condition.getAsBoolean(), "service lifecycle did not reach expected state");
    }

    @Test
    void crashRestartsButIntentionalStopDoesNot() throws Exception {
        List<FakeProcess> processes = new java.util.concurrent.CopyOnWriteArrayList<>();
        AtomicInteger registered = new AtomicInteger();
        AtomicInteger unregistered = new AtomicInteger();
        AtomicInteger notifications = new AtomicInteger();
        ServiceProcessManager.Registry registry = new ServiceProcessManager.Registry() {
            public void register(String id, long pid) { registered.incrementAndGet(); }
            public void unregister(String id) { unregistered.incrementAndGet(); }
        };
        ServiceProcessManager manager = new ServiceProcessManager(
                id -> notifications.incrementAndGet(), registry, 3, 10);
        assertTrue(manager.start("example", () -> {
            FakeProcess process = new FakeProcess(processes.size() + 1);
            processes.add(process);
            return process;
        }, ignored -> {}).startsWith("DEPLOYED_SUCCESS"));
        assertEquals("SERVICE_ALREADY_RUNNING: example",
                manager.start("example", () -> new FakeProcess(9), ignored -> {}));

        processes.get(0).finish();
        await(() -> processes.size() == 2);
        assertEquals(0, notifications.get(), "master should retain a service during recovery");
        assertEquals("STOPPED: example", manager.stop("example"));
        await(() -> notifications.get() == 1);
        assertEquals(2, registered.get());
        assertEquals(2, unregistered.get());
        Thread.sleep(40);
        assertEquals(2, processes.size(), "intentional stop must not relaunch");
    }

    @Test
    void repeatedCrashesStopAfterBoundedRestarts() throws Exception {
        List<FakeProcess> processes = new java.util.concurrent.CopyOnWriteArrayList<>();
        AtomicInteger notifications = new AtomicInteger();
        ServiceProcessManager.Registry registry = new ServiceProcessManager.Registry() {
            public void register(String id, long pid) {}
            public void unregister(String id) {}
        };
        ServiceProcessManager manager = new ServiceProcessManager(
                id -> notifications.incrementAndGet(), registry, 3, 5);
        manager.start("crashes", () -> {
            FakeProcess process = new FakeProcess(processes.size() + 1);
            processes.add(process);
            return process;
        }, ignored -> {});
        for (int attempt = 0; attempt < 4; attempt++) {
            final int current = attempt;
            await(() -> processes.size() > current);
            processes.get(attempt).finish();
        }
        await(() -> notifications.get() == 1);
        assertEquals(4, processes.size(), "initial launch plus at most three restarts");
        assertEquals("UNKNOWN_SERVICE: crashes", manager.stop("crashes"));
    }

    @Test
    void stopDuringBackoffCancelsPendingRestart() throws Exception {
        List<FakeProcess> processes = new java.util.concurrent.CopyOnWriteArrayList<>();
        AtomicInteger notifications = new AtomicInteger();
        ServiceProcessManager.Registry registry = new ServiceProcessManager.Registry() {
            public void register(String id, long pid) {}
            public void unregister(String id) {}
        };
        ServiceProcessManager manager = new ServiceProcessManager(
                id -> notifications.incrementAndGet(), registry, 3, 150);
        manager.start("pending", () -> {
            FakeProcess process = new FakeProcess(processes.size() + 1);
            processes.add(process);
            return process;
        }, ignored -> {});
        processes.get(0).finish();
        assertEquals("STOPPED: pending", manager.stop("pending"));
        await(() -> notifications.get() == 1);
        Thread.sleep(200);
        assertEquals(1, processes.size());
    }

    @Test
    void realChildProcessAlsoHonorsRestartLimit() throws Exception {
        AtomicInteger launches = new AtomicInteger();
        AtomicInteger notifications = new AtomicInteger();
        ServiceProcessManager.Registry registry = new ServiceProcessManager.Registry() {
            public void register(String id, long pid) {}
            public void unregister(String id) {}
        };
        ServiceProcessManager manager = new ServiceProcessManager(
                id -> notifications.incrementAndGet(), registry, 3, 5);
        String javaBin = System.getProperty("java.home") + File.separator + "bin"
                + File.separator + "java" + (System.getProperty("os.name").toLowerCase().contains("win")
                ? ".exe" : "");
        assertTrue(manager.start("real", () -> {
            launches.incrementAndGet();
            return new ProcessBuilder(javaBin, "-version")
                    .redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .start();
        }, ignored -> {}).startsWith("DEPLOYED_SUCCESS"));
        await(() -> notifications.get() == 1);
        assertEquals(4, launches.get());
        assertEquals("UNKNOWN_SERVICE: real", manager.stop("real"));
    }

    @Test
    void restartLaunchFailuresAreAlsoBounded() throws Exception {
        FakeProcess first = new FakeProcess(42);
        AtomicInteger launches = new AtomicInteger();
        AtomicInteger notifications = new AtomicInteger();
        ServiceProcessManager.Registry registry = new ServiceProcessManager.Registry() {
            public void register(String id, long pid) {}
            public void unregister(String id) {}
        };
        ServiceProcessManager manager = new ServiceProcessManager(
                id -> notifications.incrementAndGet(), registry, 3, 5);
        assertTrue(manager.start("failed-launch", () -> {
            if (launches.incrementAndGet() == 1) return first;
            throw new IOException("executable unavailable");
        }, ignored -> {}).startsWith("DEPLOYED_SUCCESS"));
        first.finish();
        await(() -> notifications.get() == 1);
        assertEquals(4, launches.get());
        assertEquals("UNKNOWN_SERVICE: failed-launch", manager.stop("failed-launch"));
    }
}

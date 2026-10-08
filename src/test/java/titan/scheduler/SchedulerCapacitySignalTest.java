package titan.scheduler;

import org.junit.jupiter.api.Test;
import titan.network.TitanProtocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SchedulerCapacitySignalTest {
    @SuppressWarnings("unchecked")
    private static <T> T field(Scheduler scheduler, String name) throws Exception {
        Field field = Scheduler.class.getDeclaredField(name);
        field.setAccessible(true);
        return (T) field.get(scheduler);
    }

    @Test void concurrentReservationsNeverExceedAdvertisedCapacity() throws Exception {
        Worker worker = new Worker("127.0.0.1", 8080, List.of("GENERAL"), true);
        worker.setMaxCap(4);
        worker.reconcileLoad(3, 3);
        ExecutorService pool = Executors.newFixedThreadPool(16);
        try {
            List<Future<Boolean>> attempts = new ArrayList<>();
            for (int i = 0; i < 100; i++) attempts.add(pool.submit(worker::tryReserveSlot));
            int accepted = 0;
            for (Future<Boolean> attempt : attempts) if (attempt.get()) accepted++;
            assertEquals(1, accepted);
            assertEquals(4, worker.getCurrentLoad());
            assertTrue(worker.isSaturated());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test void heartbeatCannotEraseSchedulerReservation() {
        Worker worker = new Worker("127.0.0.1", 8080, List.of("GENERAL"), true);
        worker.setMaxCap(2);
        assertTrue(worker.tryReserveSlot());
        worker.reconcileLoad(0, 0);
        assertEquals(1, worker.getReservedLoad());
        assertEquals(1, worker.getCurrentLoad());
        assertTrue(worker.tryReserveSlot());
        assertFalse(worker.tryReserveSlot());
    }

    @Test void releaseWakesSaturatedDispatcherAndSignalsCoalesce() throws Exception {
        try (ServerSocket workerSocket = new ServerSocket(0)) {
            CountDownLatch received = new CountDownLatch(1);
            Thread fakeWorker = new Thread(() -> {
                try (Socket socket = workerSocket.accept()) {
                    TitanProtocol.read(new DataInputStream(socket.getInputStream()));
                    TitanProtocol.send(new DataOutputStream(socket.getOutputStream()), TitanProtocol.OP_ACK, "DONE");
                    received.countDown();
                } catch (Exception error) { throw new AssertionError(error); }
            });
            fakeWorker.setDaemon(true);
            fakeWorker.start();

            Scheduler scheduler = new Scheduler(0, new ServiceReadinessMonitor());
            Thread dispatcher = null;
            try {
                scheduler.registerWorker("127.0.0.1", workerSocket.getLocalPort(), "GENERAL", true);
                Worker worker = scheduler.getWorkerRegistry().getWorkers().iterator().next();
                worker.setMaxCap(1);
                worker.incrementCurrentLoad();

                Job occupied = new Job("GENERAL|occupied", 1, 0);
                occupied.setId("occupied");
                occupied.setStatus(Job.Status.RUNNING);
                Map<String, Job> running = field(scheduler, "runningJobs");
                Map<String, TaskExecution> records = field(scheduler, "executionHistory");
                running.put(occupied.getId(), occupied);
                records.put(occupied.getId(), new TaskExecution(occupied.getId(), worker));

                Method loop = Scheduler.class.getDeclaredMethod("runDispatchLoop");
                loop.setAccessible(true);
                dispatcher = new Thread(() -> {
                    try { loop.invoke(scheduler); }
                    catch (Exception error) {
                        if (!(error.getCause() instanceof InterruptedException)) throw new AssertionError(error);
                    }
                });
                dispatcher.start();
                Job next = new Job("GENERAL|next", 1, 0);
                next.setId("next");
                scheduler.submitJob(next);

                Thread.sleep(100);
                long releasedAt = System.nanoTime();
                scheduler.handleJobCallback("occupied|COMPLETED|done");
                assertTrue(received.await(850, TimeUnit.MILLISECONDS),
                        "capacity release should wake dispatch before its timeout");
                assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - releasedAt) < 850);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                while (next.getStatus() != Job.Status.COMPLETED && System.nanoTime() < deadline)
                    Thread.sleep(5);
                assertEquals(Job.Status.COMPLETED, next.getStatus());

                BlockingQueue<?> signal = field(scheduler, "capacitySignal");
                for (int i = 0; i < 100; i++) scheduler.registerWorker(
                        "127.0.0.1", workerSocket.getLocalPort(), "GENERAL", true);
                assertTrue(signal.size() <= 1, "repeated wake-ups must coalesce");
            } finally {
                scheduler.stop();
                if (dispatcher != null) { dispatcher.interrupt(); dispatcher.join(2000); }
            }
        }
    }

    @Test void boundedWaitRecoversWithoutSignal() throws Exception {
        Scheduler scheduler = new Scheduler(0, new ServiceReadinessMonitor());
        try {
            Method wait = Scheduler.class.getDeclaredMethod("awaitCapacityChange");
            wait.setAccessible(true);
            long start = System.nanoTime();
            wait.invoke(scheduler);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(elapsedMs >= 850 && elapsedMs < 2000, "fallback must be bounded near one second");
        } finally { scheduler.stop(); }
    }

    @Test void registeringFirstWorkerWakesQueuedJob() throws Exception {
        try (ServerSocket workerSocket = new ServerSocket(0)) {
            CountDownLatch received = new CountDownLatch(1);
            Thread fakeWorker = new Thread(() -> {
                try (Socket socket = workerSocket.accept()) {
                    TitanProtocol.read(new DataInputStream(socket.getInputStream()));
                    TitanProtocol.send(new DataOutputStream(socket.getOutputStream()), TitanProtocol.OP_ACK, "DONE");
                    received.countDown();
                } catch (Exception error) { throw new AssertionError(error); }
            });
            fakeWorker.setDaemon(true);
            fakeWorker.start();
            Scheduler scheduler = new Scheduler(0, new ServiceReadinessMonitor());
            Thread dispatcher = null;
            try {
                Method loop = Scheduler.class.getDeclaredMethod("runDispatchLoop");
                loop.setAccessible(true);
                dispatcher = new Thread(() -> {
                    try { loop.invoke(scheduler); }
                    catch (Exception error) {
                        if (!(error.getCause() instanceof InterruptedException)) throw new AssertionError(error);
                    }
                });
                dispatcher.start();
                Job job = new Job("GENERAL|registered", 1, 0);
                scheduler.submitJob(job);
                Thread.sleep(100);
                scheduler.registerWorker("127.0.0.1", workerSocket.getLocalPort(), "GENERAL", true);
                assertTrue(received.await(850, TimeUnit.MILLISECONDS),
                        "worker registration should wake the pending dispatcher");
            } finally {
                scheduler.stop();
                if (dispatcher != null) { dispatcher.interrupt(); dispatcher.join(2000); }
            }
        }
    }

    @Test void workerCapacityRejectionRequeuesWithoutConsumingRetry() throws Exception {
        try (ServerSocket workerSocket = new ServerSocket(0)) {
            CountDownLatch accepted = new CountDownLatch(1);
            Thread fakeWorker = new Thread(() -> {
                try {
                    for (int attempt = 0; attempt < 2; attempt++) {
                        try (Socket socket = workerSocket.accept()) {
                            TitanProtocol.read(new DataInputStream(socket.getInputStream()));
                            TitanProtocol.send(new DataOutputStream(socket.getOutputStream()),
                                    attempt == 0 ? TitanProtocol.OP_ERROR : TitanProtocol.OP_ACK,
                                    attempt == 0 ? "ERROR_WORKER_SATURATED" : "DONE");
                            if (attempt == 1) accepted.countDown();
                        }
                    }
                } catch (Exception error) { throw new AssertionError(error); }
            });
            fakeWorker.setDaemon(true);
            fakeWorker.start();

            Scheduler scheduler = new Scheduler(0, new ServiceReadinessMonitor());
            Thread dispatcher = null;
            try {
                scheduler.registerWorker("127.0.0.1", workerSocket.getLocalPort(), "GENERAL", true);
                Method loop = Scheduler.class.getDeclaredMethod("runDispatchLoop");
                loop.setAccessible(true);
                dispatcher = new Thread(() -> {
                    try { loop.invoke(scheduler); }
                    catch (Exception error) {
                        if (!(error.getCause() instanceof InterruptedException)) throw new AssertionError(error);
                    }
                });
                dispatcher.start();
                Job job = new Job("GENERAL|capacity-test", 1, 0);
                job.setId("capacity-rejection-test");
                scheduler.submitJob(job);
                assertTrue(accepted.await(5, TimeUnit.SECONDS));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (job.getStatus() != Job.Status.COMPLETED && System.nanoTime() < deadline)
                    Thread.sleep(5);
                assertEquals(Job.Status.COMPLETED, job.getStatus());
                assertEquals(0, job.getRetryCount(), "declined dispatch is not an execution failure");
                assertEquals(0, scheduler.getWorkerRegistry().getWorkers().iterator().next().getCurrentLoad());
                String spans = scheduler.timelineJSON(0, 100);
                assertFalse(spans.contains("REJECTED"), "declined dispatch is not an execution attempt");
                assertEquals(1, spans.split("capacity-rejection-test", -1).length - 1,
                        "only the accepted execution should remain in the timeline");
            } finally {
                scheduler.stop();
                if (dispatcher != null) { dispatcher.interrupt(); dispatcher.join(2000); }
            }
        }
    }
}

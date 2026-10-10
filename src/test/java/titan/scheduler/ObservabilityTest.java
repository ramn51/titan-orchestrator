package titan.scheduler;

import org.junit.jupiter.api.Test;
import titan.network.TitanProtocol;
import java.io.*;
import java.lang.reflect.*;
import java.net.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class ObservabilityTest {
    @Test void registrationRetainsWorkerLoadAndIdleState() {
        WorkerRegistry registry = new WorkerRegistry();
        registry.addWorker("127.0.0.1", 8080, "GENERAL", false);
        Worker first = registry.getWorkers().iterator().next();
        first.setCurrentLoad(2);
        first.setMaxCap(8);
        registry.addWorker("127.0.0.1", 8080, "GPU", true);
        Worker second = registry.getWorkers().iterator().next();
        assertSame(first, second);
        assertEquals(2, second.getCurrentLoad());
        assertEquals(8, second.getMaxCap());
        assertTrue(second.isPermanent());
        assertTrue(second.capabilities().containsAll(List.of("GENERAL", "GPU")));
    }

    @Test void periodicReregistrationDoesNotFloodWorkerEvents() {
        Scheduler scheduler = new Scheduler(0, new ServiceReadinessMonitor());
        try {
            scheduler.registerWorker("127.0.0.1", 18081, "GENERAL", true);
            for (int i = 0; i < 25; i++)
                scheduler.registerWorker("127.0.0.1", 18081, "GENERAL", true);
            String metrics = scheduler.metricsJSON(0, 100);
            assertEquals(1, metrics.split("WORKER_REGISTERED", -1).length - 1);
        } finally {
            scheduler.stop();
        }
    }

    @Test void completedSpansAreJournaledByUtcDayWithoutBlockingCompletion() throws Exception {
        Path dir = Files.createTempDirectory("titan-observability-");
        try (ExecutionTimeline timeline = new ExecutionTimeline(dir, 2, 14, 4)) {
            Worker worker = new Worker("localhost", 8080, List.of("GENERAL"), true);
            Job job = new Job("DAG-observed", "script.py", 7, 0, List.of("DAG-parent"));
            timeline.start(job, worker, System.currentTimeMillis() - 20);
            timeline.finish(job.getId(), "FAILED", "exit 3");
            assertTrue(timeline.json().contains("\"eligible_at\":"));
            assertTrue(timeline.json().contains("\"priority\":7"));
        }
        Path journal = dir.resolve("spans-" + LocalDate.now(ZoneOffset.UTC) + ".jsonl");
        assertEquals(1, Files.readAllLines(journal).size());
        assertTrue(Files.readString(journal).contains("\"reason\":\"exit 3\""));
    }

    @Test void capabilityWaitIsVisibleAndMatchingRegistrationReleasesJob() throws Exception {
        Scheduler scheduler = new Scheduler(0, new ServiceReadinessMonitor());
        Method loop = Scheduler.class.getDeclaredMethod("runDispatchLoop");
        loop.setAccessible(true);
        Thread dispatcher = new Thread(() -> {
            try { loop.invoke(scheduler); }
            catch (InvocationTargetException error) {
                if (!(error.getCause() instanceof InterruptedException)) throw new AssertionError(error);
            } catch (Exception error) { throw new AssertionError(error); }
        });
        dispatcher.start();
        try {
            Job job = new Job("GPU|model", 5, 0);
            job.setId("DAG-GPU-model");
            scheduler.submitJob(job);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (!scheduler.queueBoardJSON(20).contains("No worker has capability GPU")
                    && System.nanoTime() < deadline) Thread.sleep(10);
            assertTrue(scheduler.queueBoardJSON(20).contains("No worker has capability GPU"));
            assertTrue(scheduler.bulkStatusJSON(job.getId()).contains("No worker has capability GPU"));
            try (ServerSocket workerSocket = new ServerSocket(0)) {
                Thread worker = new Thread(() -> {
                    try (Socket socket = workerSocket.accept()) {
                        TitanProtocol.read(new DataInputStream(socket.getInputStream()));
                        TitanProtocol.send(new DataOutputStream(socket.getOutputStream()), TitanProtocol.OP_ACK, "DONE");
                    } catch (Exception error) { throw new AssertionError(error); }
                });
                worker.setDaemon(true);
                worker.start();
                scheduler.registerWorker("127.0.0.1", workerSocket.getLocalPort(), "GPU", true);
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (job.getStatus() != Job.Status.COMPLETED && System.nanoTime() < deadline)
                    Thread.sleep(10);
                assertEquals(Job.Status.COMPLETED, job.getStatus());
            }
        } finally {
            scheduler.stop();
            dispatcher.interrupt();
            dispatcher.join(2000);
        }
    }

    @Test void queueBoardSamplesReadyAndBlockedLanesIndependently() {
        Scheduler scheduler = new Scheduler(0, new ServiceReadinessMonitor());
        try {
            List<Job> jobs = new ArrayList<>();
            for (int index = 0; index < 20; index++)
                jobs.add(new Job("queue-root-" + index, "GENERAL|noop", 1, 0, List.of()));
            for (int index = 0; index < 20; index++)
                jobs.add(new Job("queue-child-" + index, "GENERAL|noop", 1, 0,
                        List.of("queue-root-" + index)));
            scheduler.submitDag(jobs);

            String board = scheduler.queueBoardJSON(10);
            assertTrue(board.contains("\"lane\":\"ready\""), board);
            assertTrue(board.contains("\"lane\":\"blocked\""), board);
            assertTrue(board.contains("\"dependencies\":20"), board);
        } finally {
            scheduler.stop();
        }
    }
}

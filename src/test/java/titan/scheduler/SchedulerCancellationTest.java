package titan.scheduler;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import org.junit.jupiter.api.Test;
import titan.storage.TitanJRedisAdapter;
import static org.junit.jupiter.api.Assertions.*;

class SchedulerCancellationTest {
    @SuppressWarnings("unchecked")
    private static <T> T field(Scheduler scheduler, String name) throws Exception {
        Field field = Scheduler.class.getDeclaredField(name);
        field.setAccessible(true);
        return (T) field.get(scheduler);
    }

    private static void offline(Scheduler scheduler) throws Exception {
        Field field = Scheduler.class.getDeclaredField("redis");
        field.setAccessible(true);
        field.set(scheduler, new TitanJRedisAdapter(null, 0));
    }

    @Test void queuedAndDependentJobsAreCancelledTransitively() throws Exception {
        Scheduler scheduler = new Scheduler(0);
        try {
            offline(scheduler);
            Job root = new Job("DAG-cancel-root", "GENERAL|work.py", 1, 60_000, List.of());
            Job child = new Job("DAG-cancel-child", "GENERAL|work.py", 1, 0, List.of(root.getId()));
            Job grandchild = new Job("DAG-cancel-grandchild", "GENERAL|work.py", 1, 0, List.of(child.getId()));
            scheduler.submitJob(root);
            scheduler.submitJob(child);
            scheduler.submitJob(grandchild);
            assertEquals("CANCELLED:3", scheduler.cancelJob(root.getId()));
            for (Job job : List.of(root, child, grandchild)) {
                assertEquals(Job.Status.CANCELLED, scheduler.getJobStatus(job.getId()));
                assertEquals(Job.Status.CANCELLED, job.getStatus());
            }
            assertTrue(scheduler.getDAGWaitingRoom().isEmpty());
            assertTrue(((BlockingQueue<?>) field(scheduler, "waitingRoom")).isEmpty());
            scheduler.handleJobCallback(root.getId() + "|FAILED|late callback");
            assertEquals(Job.Status.CANCELLED, root.getStatus());
            assertEquals("ALREADY_TERMINAL:CANCELLED", scheduler.cancelJob(root.getId()));
        } finally {
            scheduler.stop();
        }
    }

    @Test void runningCancellationReleasesWorkerAndIgnoresLateCallback() throws Exception {
        Scheduler scheduler = new Scheduler(0);
        try {
            offline(scheduler);
            scheduler.registerWorker("127.0.0.1", 18080, "GENERAL", true);
            Worker worker = scheduler.getWorkerRegistry().getWorkers().iterator().next();
            Job job = new Job("GENERAL|work.py");
            job.setId("DAG-cancel-running");
            scheduler.submitJob(job);
            ((BlockingQueue<Job>) field(scheduler, "taskQueue")).remove(job);
            job.setStatus(Job.Status.RUNNING);
            worker.incrementCurrentLoad();
            worker.currentJobId = job.getId();
            ((Map<String, Job>) field(scheduler, "runningJobs")).put(job.getId(), job);
            ((Map<String, TaskExecution>) field(scheduler, "executionHistory"))
                    .put(job.getId(), new TaskExecution(job.getId(), worker));
            assertEquals("CANCELLED:1", scheduler.cancelJob(job.getId()));
            assertEquals(0, worker.getCurrentLoad());
            assertNull(worker.currentJobId);
            scheduler.handleJobCallback(job.getId() + "|FAILED|process killed");
            assertEquals(Job.Status.CANCELLED, scheduler.getJobStatus(job.getId()));
            assertFalse(((BlockingQueue<?>) field(scheduler, "taskQueue")).contains(job));
            assertTrue(scheduler.getSystemStatsJSON().contains("\"DAG-cancel-running\":{\"status\":\"CANCELLED\""));
        } finally {
            scheduler.stop();
        }
    }

    @Test void largeCascadeUsesDependencyIndexAndCancelsEveryNode() throws Exception {
        Scheduler scheduler = new Scheduler(0);
        try {
            offline(scheduler);
            int count = 2_000;
            List<Job> jobs = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                String id = "indexed-cancel-" + i;
                List<String> parents = i == 0 ? List.of() : List.of("indexed-cancel-" + (i - 1));
                jobs.add(new Job(id, "GENERAL|noop", 1, 60_000, parents));
            }
            scheduler.submitDag(jobs);
            long started = System.nanoTime();
            assertEquals("CANCELLED:" + count, scheduler.cancelJob(jobs.get(0).getId()));
            long elapsedMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertTrue(elapsedMs < 5_000, "indexed cancellation took " + elapsedMs + "ms");
            assertTrue(jobs.stream().allMatch(job -> job.getStatus() == Job.Status.CANCELLED));
            Map<String, java.util.Set<String>> index = field(scheduler, "childrenByParent");
            assertEquals(java.util.Set.of("indexed-cancel-1"), index.get("indexed-cancel-0"));
        } finally {
            scheduler.stop();
        }
    }
}

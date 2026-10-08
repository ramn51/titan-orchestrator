package titan.scheduler;

import org.junit.jupiter.api.Test;
import titan.storage.TitanJRedisAdapter;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class WorkerRecoveryTest {
    @SuppressWarnings("unchecked")
    private static <T> T field(Scheduler scheduler, String name) throws Exception {
        Field field = Scheduler.class.getDeclaredField(name);
        field.setAccessible(true);
        return (T) field.get(scheduler);
    }

    @Test void lostWorkerRequeuesItsAttemptAndIgnoresLateCallback() throws Exception {
        Scheduler scheduler = new Scheduler(0);
        try {
            Field redis = Scheduler.class.getDeclaredField("redis");
            redis.setAccessible(true);
            redis.set(scheduler, new TitanJRedisAdapter(null, 0));
            Worker lost = new Worker("127.0.0.1", 18101, List.of("GENERAL"), false);
            Worker replacement = new Worker("127.0.0.1", 18102, List.of("GENERAL"), false);
            Job job = new Job("script.py");
            job.setId("DAG-worker-loss-test");
            job.setStatus(Job.Status.RUNNING);
            lost.incrementCurrentLoad();
            Map<String, Job> running = field(scheduler, "runningJobs");
            Map<String, TaskExecution> records = field(scheduler, "executionHistory");
            running.put(job.getId(), job);
            records.put(job.getId(), new TaskExecution(job.getId(), lost));

            scheduler.recoverJobsForLostWorker(lost);
            assertEquals(0, lost.getCurrentLoad());
            assertEquals(1, job.getRetryCount());
            assertEquals(Job.Status.PENDING, job.getStatus());
            assertFalse(running.containsKey(job.getId()));

            job.setStatus(Job.Status.RUNNING);
            running.put(job.getId(), job);
            records.put(job.getId(), new TaskExecution(job.getId(), replacement));
            scheduler.handleJobCallback(job.getId() + "|COMPLETED|worker=18101|late result");
            assertTrue(running.containsKey(job.getId()));
            assertNotEquals(Job.Status.COMPLETED, job.getStatus());
        } finally {
            scheduler.stop();
        }
    }
}

package titan.scheduler;

import java.lang.reflect.Field;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Run with: java -cp <compiled classes> titan.scheduler.SchedulerCallbackStatusTest */
public final class SchedulerCallbackStatusTest {
    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    @SuppressWarnings("unchecked")
    private static <T> T field(Scheduler scheduler, String name) throws Exception {
        Field field = Scheduler.class.getDeclaredField(name);
        field.setAccessible(true);
        return (T) field.get(scheduler);
    }

    private static void dispatch(Scheduler scheduler, Worker worker, Job job) throws Exception {
        job.setStatus(Job.Status.RUNNING);
        worker.incrementCurrentLoad();
        worker.currentJobId = job.getId();
        Map<String, Job> running = field(scheduler, "runningJobs");
        Map<String, TaskExecution> records = field(scheduler, "executionHistory");
        running.put(job.getId(), job);
        records.put(job.getId(), new TaskExecution(job.getId(), worker));
    }

    private static void completedCallback(Scheduler scheduler, Worker worker) throws Exception {
        Job job = new Job("script.py");
        job.setId("DAG-success-test");
        dispatch(scheduler, worker, job);
        scheduler.handleJobCallback(job.getId() + "|COMPLETED|COMPLETED|0|ok");
        check(job.getStatus() == Job.Status.COMPLETED, "completed job status");
        check(worker.getCurrentLoad() == 0, "completed worker load");
        check(worker.currentJobId == null, "completed active job cleared");
        String stats = scheduler.getSystemStatsJSON();
        check(stats.contains("\"id\": \"DAG-success-test\", \"status\": \"COMPLETED\""),
                "completed worker history");
        check(stats.contains("\"DAG-success-test\":{\"status\":\"COMPLETED\""),
                "completed DAG status");
        scheduler.handleJobCallback(job.getId() + "|COMPLETED|late duplicate");
        check(worker.getCurrentLoad() == 0, "duplicate callback does not release capacity twice");
    }

    private static void rejectedCallbackAndRetries(Scheduler scheduler, Worker worker) throws Exception {
        Job job = new Job("approval.py");
        job.setId("DAG-approval-test");
        for (int attempt = 1; attempt <= 4; attempt++) {
            dispatch(scheduler, worker, job);
            scheduler.handleJobCallback(job.getId() + "|FAILED|COMPLETED|7|approval rejected");
            check(worker.getCurrentLoad() == 0, "failed worker load after attempt " + attempt);
            check(worker.currentJobId == null, "failed active job cleared");
            String stats = scheduler.getSystemStatsJSON();
            String status = attempt == 4 ? "DEAD" : "FAILED";
            check(stats.contains("\"id\": \"DAG-approval-test\", \"status\": \"" + status + "\""),
                    "worker history after attempt " + attempt);
            check(stats.contains("\"DAG-approval-test\":{\"status\":\"" + status + "\""),
                    "DAG status after attempt " + attempt);
        }
        check(job.getStatus() == Job.Status.DEAD, "rejected job final status");
    }

    private static void withScheduler(boolean rejected) throws Exception {
        Scheduler scheduler = new Scheduler(0);
        try {
            scheduler.registerWorker("127.0.0.1", 18080, "GENERAL", true);
            WorkerRegistry registry = field(scheduler, "workerRegistry");
            Worker worker = registry.getWorkers().iterator().next();
            if (rejected) rejectedCallbackAndRetries(scheduler, worker);
            else completedCallback(scheduler, worker);
        } finally {
            scheduler.stop();
        }
    }

    @Test void successfulCallbackUpdatesHistory() throws Exception { withScheduler(false); }
    @Test void rejectionAndRetriesUpdateHistory() throws Exception { withScheduler(true); }

    public static void main(String[] args) throws Exception {
        Scheduler scheduler = new Scheduler(0);
        try {
            scheduler.registerWorker("127.0.0.1", 18080, "GENERAL", true);
            WorkerRegistry registry = field(scheduler, "workerRegistry");
            Worker worker = registry.getWorkers().iterator().next();
            completedCallback(scheduler, worker);
            rejectedCallbackAndRetries(scheduler, worker);
            System.out.println("PASS SchedulerCallbackStatusTest");
        } finally {
            scheduler.stop();
        }
    }
}

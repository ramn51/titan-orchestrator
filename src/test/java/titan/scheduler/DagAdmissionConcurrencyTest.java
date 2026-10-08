package titan.scheduler;

import org.junit.jupiter.api.Test;
import titan.storage.TitanJRedisAdapter;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DagAdmissionConcurrencyTest {
    @SuppressWarnings("unchecked")
    private static <T> T field(Scheduler scheduler, String name) throws Exception {
        Field field = Scheduler.class.getDeclaredField(name);
        field.setAccessible(true);
        return (T) field.get(scheduler);
    }

    private static void offline(Scheduler scheduler) throws Exception {
        Field redis = Scheduler.class.getDeclaredField("redis");
        redis.setAccessible(true);
        redis.set(scheduler, new TitanJRedisAdapter(null, 0));
    }

    @Test void completedParentBeforeChildRegistrationIsSatisfied() throws Exception {
        Scheduler scheduler = new Scheduler(0);
        try {
            offline(scheduler);
            Job parent = new Job("parent", "GENERAL|noop", 1, 0, List.of());
            scheduler.submitJob(parent);
            parent.setStatus(Job.Status.COMPLETED);
            Job child = new Job("child", "GENERAL|noop", 1, 0, List.of("parent"));
            scheduler.submitJob(child);
            BlockingQueue<Job> queue = field(scheduler, "taskQueue");
            assertEquals(1, queue.stream().filter(job -> job == child).count());
            assertFalse(scheduler.getDAGWaitingRoom().containsKey(child.getId()));
        } finally { scheduler.stop(); }
    }

    @Test void redeployDoesNotTreatPreviousRunsParentAsCompleted() throws Exception {
        Scheduler scheduler = new Scheduler(0);
        try {
            offline(scheduler);
            Job oldParent = new Job("same-parent", "GENERAL|noop", 1, 0, List.of());
            oldParent.setStatus(Job.Status.COMPLETED);
            Map<String, Job> known = field(scheduler, "knownJobs");
            known.put(oldParent.getId(), oldParent);
            Job child = new Job("same-child", "GENERAL|noop", 1, 0, List.of("same-parent"));
            Job newParent = new Job("same-parent", "GENERAL|noop", 1, 0, List.of());
            scheduler.submitDag(List.of(child, newParent)); // intentionally child first
            assertFalse(child.isReady());
            assertTrue(scheduler.getDAGWaitingRoom().containsKey(child.getId()));
        } finally { scheduler.stop(); }
    }

    @Test void wfCommonsStyleFanInOfFiveHundredParentsReleasesEachChildOnce() throws Exception {
        Scheduler scheduler = new Scheduler(0);
        ExecutorService finishers = Executors.newFixedThreadPool(32);
        try {
            offline(scheduler);
            int parentCount = 500;
            List<String> ids = new ArrayList<>();
            List<Job> parents = new ArrayList<>();
            Map<String, Job> known = field(scheduler, "knownJobs");
            for (int i = 0; i < parentCount; i++) {
                String id = "wf-parent-" + i;
                ids.add(id);
                Job parent = new Job(id, "GENERAL|noop", 1, 0, List.of());
                parents.add(parent);
                known.put(id, parent);
            }
            // WfFormat expresses a fan-in task with a list of parent task IDs.
            Job merge = new Job("wf-merge", "GENERAL|noop", 1, 0, ids);
            Method unlock = Scheduler.class.getDeclaredMethod("unlockChildren", String.class);
            unlock.setAccessible(true);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch finished = new CountDownLatch(parentCount);
            long started = System.nanoTime();
            for (Job parent : parents) {
                finishers.submit(() -> {
                    try {
                        start.await();
                        parent.setStatus(Job.Status.COMPLETED);
                        unlock.invoke(scheduler, parent.getId());
                    } catch (Exception error) { throw new AssertionError(error); }
                    finally { finished.countDown(); }
                });
            }
            start.countDown();
            scheduler.submitJob(merge); // registration overlaps parent completions
            assertTrue(finished.await(30, TimeUnit.SECONDS));
            BlockingQueue<Job> queue = field(scheduler, "taskQueue");
            assertEquals(1, queue.stream().filter(job -> job == merge).count());
            assertTrue(merge.isReady());
            assertFalse(scheduler.getDAGWaitingRoom().containsKey(merge.getId()));
            System.out.printf("WfCommons-style fan-in admission: %d parents, %.1f ms, child queued once%n",
                    parentCount, (System.nanoTime() - started) / 1_000_000.0);
        } finally {
            finishers.shutdownNow();
            scheduler.stop();
        }
    }

    @Test void simultaneousWorkerCallbacksUnlockFanInWithoutDuplicateQueueing() throws Exception {
        Scheduler scheduler = new Scheduler(0);
        ExecutorService callbacks = Executors.newFixedThreadPool(24);
        try {
            offline(scheduler);
            int count = 240;
            List<String> ids = new ArrayList<>();
            Map<String, Job> known = field(scheduler, "knownJobs");
            Map<String, Job> running = field(scheduler, "runningJobs");
            Map<String, TaskExecution> records = field(scheduler, "executionHistory");
            for (int i = 0; i < count; i++) {
                String id = "callback-parent-" + i;
                ids.add(id);
                Job parent = new Job(id, "GENERAL|noop", 1, 0, List.of());
                parent.setStatus(Job.Status.RUNNING);
                known.put(id, parent);
                running.put(id, parent);
                records.put(id, new TaskExecution(id, null));
            }
            Job merge = new Job("callback-merge", "GENERAL|noop", 1, 0, ids);
            CountDownLatch start = new CountDownLatch(1);
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
            for (String id : ids) futures.add(callbacks.submit(() -> {
                start.await();
                scheduler.handleJobCallback(id + "|COMPLETED|ok");
                return null;
            }));
            start.countDown();
            scheduler.submitJob(merge);
            for (var future : futures) future.get(30, TimeUnit.SECONDS);
            BlockingQueue<Job> queue = field(scheduler, "taskQueue");
            assertEquals(1, queue.stream().filter(job -> job == merge).count());
            assertTrue(merge.isReady());
        } finally {
            callbacks.shutdownNow();
            scheduler.stop();
        }
    }
}

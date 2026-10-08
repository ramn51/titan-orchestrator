package titan.scheduler;

import java.util.List;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ExecutionTimelineTest {
    @Test void retainsLatestThousandSpansInDeque() throws Exception {
        ExecutionTimeline timeline = new ExecutionTimeline(Path.of("unused-timeline-test.jsonl"));
        Worker worker = new Worker("localhost", 8080, List.of("GENERAL"), true);
        for (int i = 0; i <= 1000; i++) {
            timeline.start(new Job("DAG-span-" + i, "script.py", 1, 0, List.of()), worker, 0);
        }
        String json = timeline.json();
        assertEquals(1000, json.split("\"job_id\"", -1).length - 1);
        assertFalse(json.contains("\"DAG-span-0\""));
        assertTrue(json.contains("\"DAG-span-1000\""));
    }

    @Test void recordsDistinctAttemptsAndEscapesFailureReason() throws Exception {
        Path journal = Files.createTempFile("titan-timeline-", ".jsonl");
        ExecutionTimeline timeline = new ExecutionTimeline(journal);
        Worker worker = new Worker("localhost", 8080, List.of("GENERAL"), true);
        Job job = new Job("DAG-unit", "script.py", 2, 0, List.of("DAG-parent"));
        timeline.start(job, worker, System.currentTimeMillis() - 100);
        timeline.finish(job.getId(), "FAILED", "exit 3: \"bad\"\nline");
        job.incrementRetry();
        timeline.start(job, worker, System.currentTimeMillis());
        timeline.finish(job.getId(), "COMPLETED", "");
        timeline.close();
        String json = timeline.json();
        assertTrue(json.contains("\"attempt\":1"));
        assertTrue(json.contains("\"attempt\":2"));
        assertTrue(json.contains("\"queued_at\":"));
        assertTrue(json.contains("\"parents\":[\"DAG-parent\"]"));
        assertTrue(json.contains("exit 3: \\\"bad\\\"\\nline"));
        assertEquals(2, json.split("\"job_id\"", -1).length - 1);
        assertEquals(2, Files.readAllLines(journal).size());
        Files.deleteIfExists(journal);
    }

    @Test void discardsDispatchDeclinedBeforeExecution() throws Exception {
        Path journal = Files.createTempFile("titan-timeline-discard-", ".jsonl");
        ExecutionTimeline timeline = new ExecutionTimeline(journal);
        Worker worker = new Worker("localhost", 8080, List.of("GENERAL"), true);
        Job job = new Job("DAG-declined", "script.py", 1, 0, List.of());
        timeline.start(job, worker, System.currentTimeMillis());
        timeline.discard(job.getId());
        timeline.close();
        assertFalse(timeline.json().contains(job.getId()));
        assertEquals(0, Files.size(journal));
        Files.deleteIfExists(journal);
    }
}

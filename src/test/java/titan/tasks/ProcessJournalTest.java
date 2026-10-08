package titan.tasks;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ProcessJournalTest {
    @TempDir Path temp;

    @Test void updatesPreserveOtherWorkersEntries() throws Exception {
        Path journal = temp.resolve("processes.tsv");
        ProcessJournal.setTestJournal(journal);
        try {
            long pid = ProcessHandle.current().pid();
            ProcessJournal.register("worker:1", "service-a", pid);
            ProcessJournal.register("worker:2", "service-b", pid);
            assertEquals(2, Files.readAllLines(journal).size());
            ProcessJournal.unregister("worker:1", "service-a");
            assertEquals(1, Files.readAllLines(journal).size());
            ProcessJournal.unregister("worker:2", "service-b");
            assertTrue(Files.readAllLines(journal).isEmpty());
        } finally {
            ProcessJournal.setTestJournal(null);
        }
    }
}

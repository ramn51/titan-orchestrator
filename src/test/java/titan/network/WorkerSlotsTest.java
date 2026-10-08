package titan.network;

import org.junit.jupiter.api.Test;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class WorkerSlotsTest {
    @Test void defaultsToFourSlots() {
        assertEquals(4, RpcWorkerServer.parseSlots(null));
        assertEquals(4, RpcWorkerServer.parseSlots(" "));
    }

    @Test void acceptsConfiguredCapacity() {
        assertEquals(8, RpcWorkerServer.parseSlots("8"));
        assertEquals(20, RpcWorkerServer.parseSlots(" 20 "));
    }

    @Test void rejectsInvalidCapacity() {
        for (String value : new String[] {"0", "-1", "65", "many"}) {
            assertThrows(IllegalArgumentException.class, () -> RpcWorkerServer.parseSlots(value));
        }
    }

    @Test void releasesFinishedSlotBeforeCompletionCallback() {
        AtomicInteger activeJobs = new AtomicInteger(1);

        RpcWorkerServer.releaseSlotBeforeCallback(activeJobs,
                () -> assertEquals(0, activeJobs.get(),
                        "The master callback must not observe a locally occupied finished slot"));

        assertEquals(0, activeJobs.get());
    }

    @Test void refusesToReleaseAnUnreservedSlot() {
        AtomicInteger activeJobs = new AtomicInteger(0);

        assertThrows(IllegalStateException.class,
                () -> RpcWorkerServer.releaseSlotBeforeCallback(activeJobs, () -> fail("callback must not run")));
        assertEquals(0, activeJobs.get());
    }
}

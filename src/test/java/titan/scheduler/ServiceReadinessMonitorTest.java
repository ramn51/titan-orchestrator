package titan.scheduler;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ServiceReadinessMonitorTest {
    @Test void probesUntilReadyWithoutBlockingCaller() throws Exception {
        AtomicInteger probes = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Boolean> result = new AtomicReference<>();
        try (ServiceReadinessMonitor monitor = new ServiceReadinessMonitor(
                (host, port) -> probes.incrementAndGet() == 3, 5, 10)) {
            long start = System.nanoTime();
            monitor.watch("ready", "127.0.0.1", 9999, ready -> {
                result.set(ready);
                done.countDown();
            });
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 100);
            assertTrue(done.await(2, TimeUnit.SECONDS));
            assertEquals(Boolean.TRUE, result.get());
            assertEquals(3, probes.get());
        }
    }

    @Test void givesUpAfterBoundAndCancellationSuppressesCallback() throws Exception {
        AtomicInteger probes = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(1);
        try (ServiceReadinessMonitor monitor = new ServiceReadinessMonitor(
                (host, port) -> { probes.incrementAndGet(); return false; }, 3, 10)) {
            monitor.watch("failed", "127.0.0.1", 9999, ready -> {
                assertFalse(ready);
                done.countDown();
            });
            assertTrue(done.await(2, TimeUnit.SECONDS));
            assertEquals(3, probes.get());
            AtomicInteger callbacks = new AtomicInteger();
            monitor.watch("cancelled", "127.0.0.1", 9999, ready -> callbacks.incrementAndGet());
            monitor.cancel("cancelled");
            Thread.sleep(80);
            assertEquals(0, callbacks.get());
        }
    }
}

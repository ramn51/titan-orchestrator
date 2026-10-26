package titan.network;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecutionTimingEnvelopeTest {
    @Test
    void roundTripsTimingWithoutChangingResult() {
        String wrapped = ExecutionTimingEnvelope.wrap("COMPLETED|0|answer", 11, 22, 33, 44, 55);
        ExecutionTimingEnvelope.Parsed parsed = ExecutionTimingEnvelope.parse(wrapped);

        assertEquals("COMPLETED|0|answer", parsed.result());
        assertEquals(11L, parsed.timings().get("worker_queue_ns"));
        assertEquals(22L, parsed.timings().get("preprocess_ns"));
        assertEquals(33L, parsed.timings().get("process_startup_ns"));
        assertEquals(44L, parsed.timings().get("script_ns"));
        assertEquals(55L, parsed.timings().get("callback_sent_at_ms"));
    }

    @Test
    void leavesLegacyAndMalformedResultsUntouched() {
        ExecutionTimingEnvelope.Parsed legacy = ExecutionTimingEnvelope.parse("COMPLETED|0|legacy");
        assertEquals("COMPLETED|0|legacy", legacy.result());
        assertTrue(legacy.timings().isEmpty());

        String malformed = ExecutionTimingEnvelope.PREFIX + "worker_queue_ns=nope;actual";
        ExecutionTimingEnvelope.Parsed parsed = ExecutionTimingEnvelope.parse(malformed);
        assertEquals(malformed, parsed.result());
        assertTrue(parsed.timings().isEmpty());
    }
}

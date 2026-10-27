/*
 * Copyright 2026 Ram Narayanan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package titan.network;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Optional timing metadata carried inside the existing completion result field.
 * Workers and masters that do not know this envelope remain protocol compatible.
 */
public final class ExecutionTimingEnvelope {
    static final String PREFIX = "__TITAN_TIMING__";

    private ExecutionTimingEnvelope() { }

    public static String wrap(String result, long workerQueueNanos, long preprocessNanos,
                              long processStartupNanos, long scriptNanos,
                              long callbackSentAtMillis) {
        return PREFIX + "worker_queue_ns=" + nonNegative(workerQueueNanos)
                + ",preprocess_ns=" + nonNegative(preprocessNanos)
                + ",process_startup_ns=" + nonNegative(processStartupNanos)
                + ",script_ns=" + nonNegative(scriptNanos)
                + ",callback_sent_at_ms=" + nonNegative(callbackSentAtMillis)
                + ";" + (result == null ? "" : result);
    }

    public static Parsed parse(String result) {
        if (result == null || !result.startsWith(PREFIX)) {
            return new Parsed(result == null ? "" : result, Collections.emptyMap());
        }
        int separator = result.indexOf(';', PREFIX.length());
        if (separator < 0) return new Parsed(result, Collections.emptyMap());

        Map<String, Long> timings = new LinkedHashMap<>();
        String encoded = result.substring(PREFIX.length(), separator);
        for (String item : encoded.split(",")) {
            int equals = item.indexOf('=');
            if (equals <= 0) continue;
            try {
                timings.put(item.substring(0, equals), Long.parseLong(item.substring(equals + 1)));
            } catch (NumberFormatException ignored) {
                // A malformed optional field must not invalidate a job completion.
            }
        }
        if (timings.isEmpty()) return new Parsed(result, Collections.emptyMap());
        return new Parsed(result.substring(separator + 1), Collections.unmodifiableMap(timings));
    }

    private static long nonNegative(long value) { return Math.max(0L, value); }

    public record Parsed(String result, Map<String, Long> timings) { }
}

/*
 * Copyright 2026 Ram Narayanan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */

package titan.network;

import titan.network.TitanProtocol;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LogBatcher {
    private final String jobId;
    private final String host;
    private final int port;

    private final List<String> buffer = new ArrayList<>();
    private final Object lock = new Object();

    private static final int BATCH_SIZE = 200;
    private static final int FLUSH_INTERVAL_MS = 2000;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public LogBatcher(String jobId, String host, int port) {
        this.jobId = jobId;
        this.host = host;
        this.port = port;
        // Start background flusher
        scheduler.scheduleAtFixedRate(this::flush, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public void addLog(String line) {
        synchronized (lock) {
            buffer.add(line);
            if (buffer.size() >= BATCH_SIZE) {
                flush();
            }
        }
    }

    public void flush() {
        List<String> toSend;
        synchronized (lock) {
            if (buffer.isEmpty()) return;
            toSend = new ArrayList<>(buffer);
            buffer.clear();
        }

        try (Socket socket = new Socket(host, port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {

            // Format: jobId|line1\nline2\nline3...
            String logBlock = String.join("\n", toSend);
            String payload = jobId + "|" + logBlock;

            // Use OP_LOG_BATCH (Ensure you added this int to TitanProtocol, e.g., 205)
            // If you haven't updated protocol yet, use OP_LOG_STREAM but be careful about parsing
            TitanProtocol.send(out, TitanProtocol.OP_LOG_BATCH, payload);

        } catch (Exception e) {
            System.err.println("[Batcher] Failed to send logs for " + jobId);
        }
    }

    public void close() {
        flush(); // Send whatever is left
        scheduler.shutdownNow();
    }
}
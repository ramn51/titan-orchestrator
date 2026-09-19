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
import titan.scheduler.WorkerRegistry;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * {@code RpcClient} provides functionality for making Remote Procedure Calls (RPC) to a server.
 * It handles the underlying network communication, including connecting to a host and port,
 * sending requests using the {@link titan.network.TitanProtocol}, and receiving responses.
 * <p>
 * This client is designed to interact with servers that implement the {@link titan.network.TitanProtocol}
 * for data exchange, supporting various operation codes and payloads.
 * </p>
 */
    public class RpcClient {
    WorkerRegistry workerRegistry;
//    public RpcClient(){}
    /**
 * Constructs a new {@code RpcClient} instance.
 * <p>
 * This constructor initializes the client with a {@link WorkerRegistry},
 * which might be used for internal client-side worker management or lookup,
 * though its direct usage is not explicitly shown in the provided methods.
 * </p>
 * @param workerRegistry The {@link WorkerRegistry} to associate with this client.
 */
    public RpcClient(WorkerRegistry workerRegistry){
        this.workerRegistry = workerRegistry;
    }

    /**
 * Sends a request to a specified host and port with a default operation code.
 * <p>
 * This method defaults to using {@link TitanProtocol#OP_SUBMIT_JOB} as the operation code.
 * It is a convenience method for common operations like submitting jobs where the specific
 * operation code might be implicit or less critical than the payload itself.
 * </p>
 * @param host The hostname or IP address of the server to connect to.
 * @param port The port number on the server to connect to.
 * @param payload The string payload to send as part of the request.
 * @return The response payload from the server as a {@code String}, or an error message
 *         prefixed with "ERROR:" if the server returns an error, or {@code null} if an
 *         IO error occurs indicating a potential network issue or dead worker.
 * @see #sendRequest(String, int, byte, String)
 */
    public String sendRequest(String host, int port, String payload) {
        // Defaulting to OP_SUBMIT_JOB or generic OP_DATA for legacy calls
        return sendRequest(host, port, TitanProtocol.OP_SUBMIT_JOB, payload);
    }

    /**
 * Sends a request to a specified host and port with a given operation code and payload.
 * <p>
 * This is the primary method for sending RPC requests. It establishes a socket connection,
 * sets a read timeout, sends the request using {@link TitanProtocol#send(DataOutputStream, byte, String)},
 * and then waits for and reads the server's response using {@link TitanProtocol#read(DataInputStream)}.
 * </p>
 * <p>
 * If the server returns an error ({@link TitanProtocol#OP_ERROR}), an error message is logged
 * and returned. In case of an {@link IOException}, it indicates a network problem or a dead
 * server/worker, and {@code null} is returned. Other exceptions result in a client-side error message.
 * </p>
 * @param host The hostname or IP address of the server to connect to.
 * @param port The port number on the server to connect to.
 * @param opCode The operation code (e.g., {@link TitanProtocol#OP_SUBMIT_JOB}) indicating the type of request.
 * @param payload The string payload to send as part of the request.
 * @return The response payload from the server as a {@code String}, or an error message
 *         prefixed with "ERROR:" if the server returns an error, or {@code null} if an
 *         IO error occurs indicating a potential network issue or dead worker.
 */
    public String sendRequest(String host, int port, byte opCode, String payload){
        String key = host + ":" + port;

        // First attempt reuses a pooled connection when one is available. A pooled socket can be
        // half-open without the client knowing: the worker may have closed it while idle, and that
        // only surfaces on the next write or read. So a failure on a POOLED connection is not
        // evidence of a dead worker, and the call is retried exactly once on a brand new socket.
        // Only a failure on that fresh socket returns null.
        //
        // This distinction is load-bearing. null is what the Scheduler reads as "worker
        // unreachable", which marks the node dead and re-queues its in-flight jobs. Treating a
        // stale pooled socket as a death would re-run healthy work.
        Conn pooled = borrow(key);
        if (pooled != null) {
            try {
                return exchange(pooled, opCode, payload, key);
            } catch (IOException stale) {
                closeQuietly(pooled);
                // fall through to a fresh connection
            } catch (Exception e) {
                closeQuietly(pooled);
                e.printStackTrace();
                return "ERROR_CLIENT: " + e.getMessage();
            }
        }

        Conn fresh = null;
        try {
            fresh = new Conn(new Socket(host, port));
            return exchange(fresh, opCode, payload, key);
        } catch (IOException e) {
            closeQuietly(fresh);
            System.err.println("[RpcClient] IO Error to " + host + ":" + port + " -> " + e.getMessage());
            return null; // Signals a dead worker/main.java.titan.network issue
        }
        catch (Exception e){
            closeQuietly(fresh);
            e.printStackTrace();
            return "ERROR_CLIENT: " + e.getMessage();
        }
    }

    /**
     * Sends one request and reads one reply on the given connection, returning it to the pool when
     * the exchange completes cleanly. The timeout is set per request rather than per socket,
     * because a reused connection would otherwise inherit the previous caller's timeout.
     */
    private String exchange(Conn c, byte opCode, String payload, String key) throws Exception {
        c.socket.setSoTimeout(30000); // no reply within 30s counts as a failure, as before
        TitanProtocol.send(c.out, opCode, payload);
        TitanProtocol.TitanPacket response = TitanProtocol.read(c.in);

        if (response.opCode == TitanProtocol.OP_ERROR) {
            release(key, c);
            System.err.println("[RpcClient] Server returned error: " + response.payload);
            return "ERROR: " + response.payload;
        }
        release(key, c);
        return response.payload;
    }

    /** A live socket with its streams, kept open between requests to the same destination. */
    private static final class Conn {
        final Socket socket; final DataOutputStream out; final DataInputStream in;
        Conn(Socket s) throws IOException {
            this.socket = s;
            this.out = new DataOutputStream(s.getOutputStream());
            this.in  = new DataInputStream(s.getInputStream());
        }
        boolean usable() { return socket != null && socket.isConnected() && !socket.isClosed(); }
    }

    /**
     * Idle connections per destination. The worker's client handler already loops on one socket
     * until it closes, so reuse needs no protocol change and no worker change: only the client
     * stopped closing after a single exchange.
     */
    private static final java.util.Map<String, java.util.concurrent.ConcurrentLinkedDeque<Conn>> POOL
            = new java.util.concurrent.ConcurrentHashMap<>();

    /** Idle sockets kept per destination. Beyond this they are closed rather than pooled. */
    private static final int MAX_IDLE_PER_HOST =
            titan.TitanConfig.getInt("titan.rpc.pool.max.idle", 8);

    private static Conn borrow(String key) {
        java.util.concurrent.ConcurrentLinkedDeque<Conn> q = POOL.get(key);
        if (q == null) return null;
        Conn c;
        while ((c = q.pollFirst()) != null) {
            if (c.usable()) return c;
            closeQuietly(c);
        }
        return null;
    }

    private static void release(String key, Conn c) {
        if (c == null || !c.usable()) { closeQuietly(c); return; }
        java.util.concurrent.ConcurrentLinkedDeque<Conn> q =
                POOL.computeIfAbsent(key, k -> new java.util.concurrent.ConcurrentLinkedDeque<>());
        if (q.size() >= MAX_IDLE_PER_HOST) { closeQuietly(c); return; }
        q.addFirst(c); // most-recently-used first: the likeliest to still be open
    }

    private static void closeQuietly(Conn c) {
        if (c == null) return;
        try { c.socket.close(); } catch (Exception ignored) { }
    }

    /** Drops every pooled connection to a destination. Called when a worker leaves the fleet. */
    public static void evict(String host, int port) {
        java.util.concurrent.ConcurrentLinkedDeque<Conn> q = POOL.remove(host + ":" + port);
        if (q == null) return;
        Conn c;
        while ((c = q.pollFirst()) != null) closeQuietly(c);
    }

}

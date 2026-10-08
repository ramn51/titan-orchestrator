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
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

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
    private static final int MAX_CONNECTIONS_PER_DESTINATION = 4;
    private final Map<String, Pool> pools = new ConcurrentHashMap<>();

    private static final class Pool {
        final Semaphore capacity = new Semaphore(MAX_CONNECTIONS_PER_DESTINATION, true);
        final ArrayDeque<Socket> idle = new ArrayDeque<>();
        boolean closed;

        synchronized Socket borrow() { return idle.pollFirst(); }
        synchronized void recycle(Socket socket) {
            if (closed || socket.isClosed()) closeQuietly(socket);
            else idle.addFirst(socket);
        }
        synchronized void close() {
            closed = true;
            while (!idle.isEmpty()) closeQuietly(idle.removeFirst());
        }
    }
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
        String destination = host + ":" + port;
        Pool pool = pools.computeIfAbsent(destination, key -> new Pool());
        boolean acquired = false;
        try {
            acquired = pool.capacity.tryAcquire(30, TimeUnit.SECONDS);
            if (!acquired) throw new IOException("RPC connection pool exhausted");
            Socket socket = pool.borrow();
            boolean reused = socket != null;
            if (socket == null) socket = connect(host, port);
            try {
                String response = exchange(socket, opCode, payload);
                pool.recycle(socket);
                return response;
            } catch (IOException firstFailure) {
                closeQuietly(socket);
                if (!reused) throw firstFailure;
                // A reused socket may have gone stale while idle. Try the request
                // once more on a new connection before reporting worker loss.
                Socket fresh = connect(host, port);
                try {
                    String response = exchange(fresh, opCode, payload);
                    pool.recycle(fresh);
                    return response;
                } catch (IOException retryFailure) {
                    closeQuietly(fresh);
                    throw retryFailure;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (IOException e) {
            System.err.println("[RpcClient] IO Error to " + destination + " -> " + e.getMessage());
            return null;
        } finally {
            if (acquired) pool.capacity.release();
        }
    }

    private static Socket connect(String host, int port) throws IOException {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port), 3000);
            socket.setSoTimeout(30000);
            return socket;
        } catch (IOException e) {
            closeQuietly(socket);
            throw e;
        }
    }

    private static String exchange(Socket socket, byte opCode, String payload) throws IOException {
        TitanProtocol.send(new DataOutputStream(socket.getOutputStream()), opCode, payload);
        try {
            TitanProtocol.TitanPacket response = TitanProtocol.read(new DataInputStream(socket.getInputStream()));
            return response.opCode == TitanProtocol.OP_ERROR ? "ERROR: " + response.payload : response.payload;
        } catch (IOException error) {
            throw error;
        } catch (Exception error) {
            throw new IOException("Invalid RPC response", error);
        }
    }

    public void evict(String host, int port) {
        Pool pool = pools.remove(host + ":" + port);
        if (pool != null) pool.close();
    }

    public void close() {
        for (String destination : pools.keySet()) {
            Pool pool = pools.remove(destination);
            if (pool != null) pool.close();
        }
    }

    private static void closeQuietly(Socket socket) {
        try { socket.close(); } catch (IOException ignored) { }
    }

}

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
package titan.storage;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * A synchronous adapter to connect Titan to RedisJava.
 * Implements the RESP (Redis Serialization Protocol) for sending commands.
 */
/**
 * A synchronous adapter to connect Titan to RedisJava.
 * Implements the RESP (Redis Serialization Protocol) for sending commands.
 * This class provides a low-level client for interacting with a Redis server,
 * handling connection management and RESP serialization/deserialization.
 */
    public class TitanJRedisAdapter implements AutoCloseable {
    private final String host;
    private final int port;
    private Socket socket;
    private OutputStream out;
    private BufferedInputStream in;
    private boolean isConnected;

    /** Successful reconnects since start. A climbing count means the link is flapping. */
    private final java.util.concurrent.atomic.AtomicLong reconnects = new java.util.concurrent.atomic.AtomicLong();

    /** @return Number of times this adapter has re-established a dropped connection. */
    public long getReconnectCount() { return reconnects.get(); }

    /** @return The configured store host, for display. */
    public String getHost() { return host; }

    /** @return The configured store port, for display. */
    public int getPort() { return port; }

    /**
     * Constructs a new {@code TitanJRedisAdapter} instance.
     * This constructor initializes the adapter but does not establish a connection to Redis.
     * Call {@link #connect()} to establish the connection.
     *
     * @param host The hostname or IP address of the Redis server.
     * @param port The port number of the Redis server.
     */
    public TitanJRedisAdapter(String host, int port) {
        this.host = host;
        this.port = port;
        isConnected = false;
    }

    /**
     * Establishes a connection to the Redis server using the configured host and port.
     * If the host is null or empty, or the port is invalid (less than or equal to 0), the adapter will operate
     * in an "in-memory" mode, meaning no connection is attempted and all operations will fail
     * or return default values, simulating a disconnected state.
     * If a connection fails, the adapter also falls back to an "in-memory" (disconnected) mode.
     *
     * @throws IOException If an I/O error occurs during socket creation or stream setup,
     *                     though most connection errors are caught and result in a disconnected state.
     */
    public void connect() throws IOException {

        if (host == null || host.isEmpty() || port <= 0) {
            System.out.println("[INFO] No Redis config found. Running in IN-MEMORY mode (No Persistence).");
            this.isConnected = false;
            return;
        }


        try {
            this.socket = new Socket(host, port);
            this.socket.setTcpNoDelay(true); // For low latency
            this.out = socket.getOutputStream();
            this.in = new BufferedInputStream(socket.getInputStream());
            this.isConnected = true;
            System.out.println("[INFO][SUCCESS] Connected to Redis for Persistence.");
        } catch (IOException e) {
            System.err.println("[FATAL][FAILED] Could not connect to Redis (" + e.getMessage() + "). Running in IN-MEMORY mode.");
            this.isConnected = false;
        }
    }

    /**
     * Checks if the adapter is currently connected to the Redis server.
     *
     * @return {@code true} if connected, {@code false} otherwise.
     */
    public boolean isConnected(){
        return this.isConnected;
    }

    /**
     * Attempts to re-establish the Redis connection if it is currently disconnected.
     * Called automatically before every operation. Silently skips if host/port are not configured.
     */
    private synchronized void tryReconnect() {
        if (isConnected) return;
        if (host == null || host.isEmpty() || port <= 0) return;
        try {
            if (socket != null && !socket.isClosed()) {
                try { socket.close(); } catch (IOException ignored) {}
            }
            this.socket = new Socket(host, port);
            this.socket.setTcpNoDelay(true);
            this.out = socket.getOutputStream();
            this.in  = new BufferedInputStream(socket.getInputStream());
            this.isConnected = true;
            reconnects.incrementAndGet();
            System.out.println("[INFO][RECONNECT] Re-connected to Redis.");
        } catch (IOException e) {
            this.isConnected = false;
        }
    }

    /**
     * Executes a raw Redis command by sending it to the server and reading its response.
     * This method handles the RESP serialization of the command arguments and deserialization of the response.
     * It is synchronized to ensure only one command is processed at a time, maintaining command-response order.
     *
     * @param args The command and its arguments, e.g., "SET", "mykey", "myvalue".
     * @return The deserialized response from Redis. The type of the returned object depends on the Redis command
     *         and its response type (e.g., String for simple strings, Long for integers, List for arrays, null for null bulk strings).
     * @throws IOException If an I/O error occurs during sending the command or reading the response,
     *                     or if Redis returns an error response.
     */
    public synchronized Object execute(String... args) throws IOException {
        sendCommand(args);
        return readResponse();
    }

    public String get(String key) throws IOException {
        tryReconnect();
        if (!isConnected) return null;
        try {
            return (String) execute("GET", key);
        } catch (IOException e) {
            this.isConnected = false;
            return null;
        }
    }

    public String set(String key, String value) throws IOException {
        tryReconnect();
        if (!isConnected) return null;
        try {
            return (String) execute("SET", key, value);
        } catch (IOException e) {
            System.err.println("[INFO][FAILED] Redis Write Failed: " + e.getMessage());
            this.isConnected = false;
            return null;
        }
    }

    public long sadd(String key, String member) throws IOException {
        tryReconnect();
        if (!isConnected) return 0;
        try {
            Object res = execute("SADD", key, member);
            if (res instanceof Long) return (Long) res;
            return 0;
        } catch (IOException e) {
            System.err.println("[INFO][FAILED] Redis SADD failed: " + e.getMessage());
            this.isConnected = false;
            return 0;
        }
    }

    @SuppressWarnings("unchecked")
    public Set<String> smembers(String key) throws IOException {
        tryReconnect();
        if (!isConnected) return Collections.emptySet();
        try {
            Object res = execute("SMEMBERS", key);
            if (res instanceof List) {
                return new HashSet<>((List<String>) res);
            }
        } catch (IOException e) {
            System.err.println("[INFO][FAILED] Redis SMEMBERS failed: " + e.getMessage());
            this.isConnected = false;
        }
        return Collections.emptySet();
    }

    public long srem(String key, String member) {
        tryReconnect();
        if (!isConnected) return 0;
        try {
            Object res = execute("SREM", key, member);
            if (res instanceof Long) return (Long) res;
            return 0;
        } catch (IOException e) {
            System.err.println("[INFO][FAILED] Redis SREM failed: " + e.getMessage());
            this.isConnected = false;
            return 0;
        }
    }


    /**
     * Serializes and sends a Redis command to the server using the RESP protocol.
     * This method constructs the RESP array format from the given arguments and writes it to the output stream.
     *
     * @param args The command and its arguments to be sent.
     * @throws IOException If an I/O error occurs while writing to the output stream.
     */
    private void sendCommand(String... args) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(args.length).append("\r\n");
        for (String arg : args) {
            sb.append("$").append(arg.length()).append("\r\n");
            sb.append(arg).append("\r\n");
        }
        out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    /**
     * Reads and deserializes a single RESP response from the Redis server.
     * This method determines the type of the response (Simple String, Error, Integer, Bulk String, Array)
     * based on the first byte and delegates to appropriate helper methods for parsing.
     *
     * @return The deserialized Redis response as a Java object (String, Long, List, or null).
     * @throws IOException If an I/O error occurs while reading from the input stream, or if Redis returns an error response,
     *                     or if an unknown RESP type is encountered.
     */
    private Object readResponse() throws IOException {
        int b = in.read();
        if (b == -1) throw new IOException("Connection closed by RedisJava");

        char type = (char) b;
        switch (type) {
            case '+': // Simple String (e.g., +OK)
                return readLine();
            case '-': // Error
                throw new IOException("RedisJava Error: " + readLine());
            case ':': // Integer
                return Long.parseLong(readLine());
            case '$': // Bulk String
                return readBulkString();
            case '*': // Array
                return readArray();
            default:
                throw new IOException("Unknown RESP type: " + type);
        }
    }

    /**
     * Reads a line from the input stream until a CR-LF sequence is encountered.
     * This is a helper method for parsing RESP simple strings, errors, and integer responses.
     *
     * @return The string read from the stream, excluding the CR-LF terminator.
     * @throws IOException If an I/O error occurs while reading from the input stream.
     */
    private String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                in.read();
                break;
            }
            sb.append((char) b);
        }
        return sb.toString();
    }

    /**
     * Reads a RESP Bulk String response from the input stream.
     * This method first reads the length of the bulk string, then reads the specified number of bytes,
     * and finally consumes the trailing CR-LF.
     *
     * @return The deserialized bulk string, or {@code null} if Redis sent a null bulk string (length -1).
     * @throws IOException If an I/O error occurs while reading from the input stream, or if the stream ends unexpectedly.
     */
    private String readBulkString() throws IOException {
        String lenStr = readLine();
        int len = Integer.parseInt(lenStr);
        if (len == -1) return null; // Null Bulk String

        byte[] data = new byte[len];
        int totalRead = 0;
        while (totalRead < len) {
            int read = in.read(data, totalRead, len - totalRead);
            if (read == -1) throw new IOException("Unexpected end of stream");
            totalRead += read;
        }
        in.read(); // consume \r
        in.read(); // consume \n
        return new String(data, StandardCharsets.UTF_8);
    }

    /**
     * Reads a RESP Array response from the input stream.
     * This method first reads the number of elements in the array, then recursively calls {@link #readResponse()}
     * for each element to deserialize them.
     *
     * @return A {@link List} of objects representing the elements of the array, or {@code null} if Redis sent a null array (count -1).
     * @throws IOException If an I/O error occurs while reading from the input stream.
     */
    private List<Object> readArray() throws IOException {
        String countStr = readLine();
        int count = Integer.parseInt(countStr);
        if (count == -1) return null;

        List<Object> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(readResponse());
        }
        return list;
    }

    @Override
    /**
     * Closes the connection to the Redis server.
     * This method is part of the {@link AutoCloseable} interface, allowing the adapter to be used in try-with-resources statements.
     *
     * @throws IOException If an I/O error occurs while closing the socket.
     */
    public void close() throws IOException {
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
}
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

package titan.scheduler;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a worker node in the scheduling system. A worker has a host, port, and a set of capabilities.
 * It maintains its current load, maximum capacity, and last seen timestamp to facilitate job assignment
 * and resource management. Workers can be permanent or temporary.
 */
    public class Worker {
    /**
 * The hostname or IP address of the worker.
 */
    private final String host;
    /**
 * The port number on which the worker is listening.
 */
    private final int port;
    /**
 * A list of capabilities or features supported by this worker.
 * These capabilities can be used by the scheduler to match jobs to suitable workers.
 */
    private final List<String> capabilities;

    // Mutable State
    /**
 * The timestamp (in milliseconds) when this worker was last seen or updated.
 * Used for liveness checks and identifying stale workers.
 */
    private long lastSeen;
    /**
 * The current number of jobs or tasks actively being processed by this worker.
 */
    private int currentLoad;
    /**
 * The maximum number of jobs or tasks this worker can handle concurrently.
 * This field will eventually be replaced by {@link #MAX_SLOTS}.
 */
    private int maxCap;
    /**
 * The ID of the job currently assigned to this worker, if any.
 * This field is public for direct access by the scheduler.
 */
    public String currentJobId = null;
    // This will replace maxCap
    /**
 * The maximum number of concurrent job slots available on any worker.
 * This is a static constant representing the default or system-wide maximum capacity.
 * This will replace the instance-specific {@code maxCap} field.
 */
    public static final int MAX_SLOTS = 4;
    /**
 * The timestamp (in milliseconds) when the worker became idle (currentLoad dropped to 0).
 * A value of -1 indicates the worker is currently busy.
 */
    private long idleStartTime = -1;
    /**
 * Indicates whether this worker is a permanent part of the cluster or a temporary,
 * dynamically provisioned worker. Permanent workers might have different lifecycle management.
 */
    private boolean isPermanent;


    /**
 * Constructs a new Worker instance.
 * Initializes the worker with its network details, capabilities, and sets its initial state.
 *
 * @param host The hostname or IP address of the worker.
 * @param port The port number on which the worker is listening.
 * @param capabilities A list of capabilities supported by this worker. Can be null, in which case an empty list is used.
 * @param isPermanent True if this worker is a permanent part of the cluster, false otherwise.
 */
    public Worker(String host, int port, List<String> capabilities, boolean isPermanent) {
        this.host = host;
        this.port = port;
        this.capabilities = (capabilities == null) ? new ArrayList<>() : capabilities;
        this.lastSeen = System.currentTimeMillis();
        this.currentLoad = 0;
        maxCap = 4;
        this.isPermanent = isPermanent;
        this.idleStartTime = System.currentTimeMillis();
    }

    /**
 * Updates the {@link #lastSeen} timestamp to the current system time.
 * This method is synchronized to ensure thread-safe updates to the worker's liveness status.
 */
    public synchronized void updateLastSeen() {
        this.lastSeen = System.currentTimeMillis();
    }

    /**
 * Checks if this worker is designated as a permanent worker.
 *
 * @return {@code true} if the worker is permanent, {@code false} otherwise.
 */
    public boolean isPermanent() {
        return isPermanent;
    }

    /**
 * Sets the current load of the worker.
 * If the load becomes 0, it records the {@link #idleStartTime}.
 * If the load is greater than 0, it resets {@link #idleStartTime} to -1.
 * This method is synchronized to ensure thread-safe updates to the worker's load and idle state.
 *
 * @param load The new current load value. Must be non-negative.
 */
    public synchronized void setCurrentLoad(int load) {
        this.currentLoad = load;
        if (load == 0) {
            if (this.idleStartTime == -1) {
                this.idleStartTime = System.currentTimeMillis();
            }
        } else {
            // Reset this once it gets a work immediately.
            this.idleStartTime = -1;
        }
    }

    /**
 * Sets the maximum capacity of the worker.
 * This method is synchronized to ensure thread-safe updates to the worker's capacity.
 *
 * @param maxCap The new maximum capacity value.
 */
    public synchronized void setMaxCap(int maxCap){
        this.maxCap = maxCap;
    }

    /**
 * Returns the maximum capacity of the worker.
 * This method is synchronized to ensure thread-safe access to the worker's capacity.
 *
 * @return The maximum number of jobs or tasks this worker can handle concurrently.
 */
    public synchronized int getMaxCap(){
        return this.maxCap;
    }

    /**
 * Calculates and returns the duration (in milliseconds) for which the worker has been idle.
 * If the worker is currently busy ({@link #idleStartTime} is -1), it returns 0.
 *
 * @return The idle duration in milliseconds, or 0 if the worker is currently busy.
 */
    public long getIdleDuration() {
        if (idleStartTime == -1) return 0;
        return System.currentTimeMillis() - idleStartTime;
    }

    /**
 * Returns the current load of the worker.
 * This method is synchronized to ensure thread-safe access to the worker's load.
 *
 * @return The current number of jobs or tasks actively being processed.
 */
    public synchronized int getCurrentLoad() {
        return currentLoad;
    }

    /**
 * Increments the current load of the worker by one.
 * Resets the {@link #idleStartTime} if it was previously set, as the worker is now busy.
 * This method is synchronized to ensure thread-safe load updates.
 */
    synchronized public void incrementCurrentLoad(){
        this.currentLoad++;
    }

    /**
 * Decrements the current load of the worker by one, if the load is greater than zero.
 * If the load drops to zero after decrementing, it records the {@link #idleStartTime}.
 * This method is synchronized to ensure thread-safe load updates and idle state management.
 */
    synchronized public void decrementCurrentLoad(){
        if(this.currentLoad > 0){
            this.currentLoad--;
        }

        if (this.currentLoad == 0) {
            this.idleStartTime = System.currentTimeMillis();
        }
    }

    /**
 * Checks if the worker is saturated, meaning its current load has reached or exceeded its maximum capacity.
 * This method is synchronized to ensure thread-safe access to load and capacity.
 *
 * @return {@code true} if the worker is saturated, {@code false} otherwise.
 */
    public synchronized boolean isSaturated(){
        return getCurrentLoad() >= maxCap;
    }

    /**
 * Returns the hostname or IP address of the worker.
 * This is a record-style accessor for the {@link #host} field.
 *
 * @return The host string.
 */
    public String host() { return host; }
    /**
 * Returns the port number on which the worker is listening.
 * This is a record-style accessor for the {@link #port} field.
 *
 * @return The port number.
 */
    public int port() { return port; }
    /**
 * Returns the timestamp (in milliseconds) when this worker was last seen or updated.
 * This is a record-style accessor for the {@link #lastSeen} field.
 *
 * @return The last seen timestamp.
 */
    public long lastSeen() { return lastSeen; }
    /**
 * Returns an unmodifiable list of capabilities supported by this worker.
 * This is a record-style accessor for the {@link #capabilities} field.
 *
 * @return An unmodifiable list of capabilities.
 */
    public List<String> capabilities() { return capabilities; }

    /**
 * Returns a string representation of the Worker, including its host, port, and current load.
 *
 * @return A formatted string representing the worker.
 */
    @Override
    public String toString() {
        return host + ":" + port + " [Load=" + currentLoad + "]";
    }
}
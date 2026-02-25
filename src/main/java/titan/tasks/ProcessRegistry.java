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

package titan.tasks;
// This is for cleaning up processes that were zombied due to abrupt worker node shutdown/crash.

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A registry for managing and cleaning up processes, particularly those that might become
 * "zombied" due to abrupt worker node shutdowns or crashes. This class provides mechanisms
 * to register running processes, unregister them, and on startup, load previously registered
 * processes from a persistent file to check for and forcibly terminate any lingering zombie processes.
 * <p>
 * The registry maintains an in-memory map of service IDs to process IDs (PIDs) and persists
 * this information to a local file to ensure state can be recovered across restarts.
 * </p>
 */
    public class ProcessRegistry {
    /**
 * An in-memory concurrent map that stores the mapping from a service identifier to its process ID (PID).
 * This map is used to keep track of active processes that should be persisted to disk.
 */
    private static final Map<String, Long> persistentMap = new ConcurrentHashMap<>();
    /**
 * The name of the file used for persisting the process registry data to disk.
 * This file stores service ID to PID mappings, allowing the system to recover
 * and clean up processes after a restart.
 */
    private static final String PROCESS_REGISTRY_FILE = "titan_tasks.db";

    /**
 * Loads the process registry from the persistent file, checks for any processes
 * that are still running (potential zombies), and forcibly terminates them.
 * After checking and cleaning up, the persistent file is cleared to ensure a fresh start.
 * <p>
 * This method is typically called during application startup to handle cases where
 * processes might have been left running due to an unexpected shutdown.
 * </p>
 * Any errors encountered during file reading or process termination are logged
 * but do not prevent the application from starting.
 */
    public static void loadAndCleanUpProcesses(){
            File f = new File(PROCESS_REGISTRY_FILE);
            if(!f.exists()) return;

        System.out.println("[INFO] [ZOMBIE KILLER] Checking for leftover processes...");

        try(BufferedReader br = new BufferedReader(new FileReader(f))){
            String line;

            while((line = br.readLine())!=null){
                String[] parts = line.split(",");
                if(parts.length == 2){
                    String serviceId = parts[0];
                    try {
                        long pid = Long.parseLong(parts[1]);

                        ProcessHandle.of(pid).ifPresent(ph -> {
                            System.out.println("[INFO] [ZOMBIE KILLER] Found Zombie (PID: " + pid + ") - " + serviceId + ". Killing it.");
                            ph.destroyForcibly();
                        });
                    } catch (NumberFormatException e) {
                        System.err.println("[WARN] Corrupt entry in PID file, skipping: " + line);
                    }
                }
            }
            // Once the cleanup is done, clear the file
            new FileWriter(f, false).close();
        } catch (IOException e) {
            // If we can't read the file at all, that's a bigger issue,
            // but we probably shouldn't crash the worker for it.
            System.err.println("[ERROR] Failed to read process registry: " + e.getMessage());
        }

        //Clear the file to start fresh
        try {
            new FileWriter(f, false).close();
        } catch (IOException e) {
            System.err.println("[ERROR] Failed to clear process registry: " + e.getMessage());
        }
    }

    /**
 * Registers a new process with the registry. The service ID and its corresponding PID
 * are stored in the in-memory map and immediately persisted to disk.
 *
 * @param serviceId The unique identifier for the service or task.
 * @param pid The process ID (PID) of the running process associated with the service.
 */
    public static void register(String serviceId, long pid) {
        persistentMap.put(serviceId, pid);
        saveToDisk();
    }

    /**
 * Unregisters a process from the registry. The entry corresponding to the given service ID
 * is removed from the in-memory map, and the updated state is immediately persisted to disk.
 *
 * @param serviceId The unique identifier of the service or task to unregister.
 */
    public static void unregister(String serviceId) {
        persistentMap.remove(serviceId);
        saveToDisk();
    }

    /**
 * Persists the current state of the in-memory process registry to the designated file.
 * Each entry (service ID and PID) is written as a comma-separated line.
 * This method is synchronized to prevent concurrent writes to the file.
 */
    private static synchronized void saveToDisk() {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(PROCESS_REGISTRY_FILE))) {
            for (Map.Entry<String, Long> entry : persistentMap.entrySet()) {
                bw.write(entry.getKey() + "," + entry.getValue());
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}

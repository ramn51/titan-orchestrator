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

public class ProcessRegistry {
    private static final Map<String, Long> persistentMap = new ConcurrentHashMap<>();
    private static final String PROCESS_REGISTRY_FILE = "titan_tasks.db";

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

    public static void register(String serviceId, long pid) {
        persistentMap.put(serviceId, pid);
        saveToDisk();
    }

    public static void unregister(String serviceId) {
        persistentMap.remove(serviceId);
        saveToDisk();
    }

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

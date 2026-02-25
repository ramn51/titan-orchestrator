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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A central registry for managing {@link Worker} instances within the Titan scheduler system.
 * This class provides mechanisms to add, retrieve, update, and remove workers,
 * facilitating the discovery and management of available processing capabilities.
 * It uses a {@link ConcurrentHashMap} to ensure thread-safe operations.
 */
    public class WorkerRegistry {
    /**
 * A thread-safe map storing {@link Worker} instances, keyed by a unique string
 * generated from the worker's host and port (e.g., "host:port").
 */
    private final Map<String, Worker> workerMap;

    /**
 * Constructs a new {@code WorkerRegistry} and initializes the internal worker map.
 */
    public WorkerRegistry(){
        this.workerMap = new ConcurrentHashMap<>();
    }

    /**
 * Returns the underlying map of workers. This map is a live view of the registry's state.
 * Modifications to the returned map will directly affect the registry.
 *
 * @return A {@link Map} where keys are worker identifiers (host:port) and values are {@link Worker} objects.
 */
    public Map<String, Worker> getWorkerMap(){
        return this.workerMap;
    }

    /**
 * Generates a unique key for a worker based on its host and port.
 * This key is used internally to identify workers in the registry.
 *
 * @param host The hostname or IP address of the worker.
 * @param port The port number the worker is listening on.
 * @return A string representing the unique key for the worker (e.g., "localhost:8080").
 */
    public String getWorkerKey(String host, int port){
        return generateKey(host, port);
    }

    /**
 * Adds a new worker to the registry or updates an existing worker's capabilities and permanence status.
 * If a worker with the given host and port already exists, its capabilities list will be updated
 * to include the new capability if it's not already present. The `isPermanent` status will be
 * set to true if the worker was previously marked permanent, ensuring permanence is sticky.
 *
 * @param host The hostname or IP address of the worker.
 * @param port The port number the worker is listening on.
 * @param capability A specific skill or capability that the worker possesses.
 * @param isPermanent {@code true} if the worker should be considered permanent and not removed by cleanup processes;
 *                    {@code false} otherwise. Once a worker is marked permanent, it remains permanent.
 */
    public void addWorker(String host, int port, String capability, boolean isPermanent){
        String key = generateKey(host, port);
        workerMap.compute(key, (k, existingWorker) -> {
           long now = System.currentTimeMillis();
           List<String> newCapabilities = new ArrayList<>();
           if(existingWorker != null){
               newCapabilities.addAll(existingWorker.capabilities());
           }

           if(!newCapabilities.contains(capability)){
               newCapabilities.add(capability);
           }

            boolean finalPermanentStatus = isPermanent;
            if (existingWorker != null && existingWorker.isPermanent()) {
                finalPermanentStatus = true; // Once permanent, stays permanent (safer)
            }

           return new Worker(host, port, newCapabilities, finalPermanentStatus);
        });
    }

    /**
 * Retrieves a collection of all currently registered workers.
 *
 * @return A {@link Collection} of {@link Worker} objects currently in the registry.
 */
    public Collection<Worker> getWorkers(){
        return workerMap.values();
    }

    /**
 * A private helper method to generate a consistent key for a worker.
 *
 * @param host The hostname or IP address of the worker.
 * @param port The port number the worker is listening on.
 * @return A string key in the format "host:port".
 */
    private String generateKey(String host, int port) {
        return host + ":" + port;
    }

    /**
 * Updates the 'last seen' timestamp for a specific worker.
 * This method is typically called when a heartbeat or check-in is received from a worker,
 * indicating it is still active.
 *
 * @param host The hostname or IP address of the worker.
 * @param port The port number the worker is listening on.
 */
    public void updateLastSeen(String host, int port){
//        String key = generateKey(host, port);
//        Worker old = workerMap.get(key);
//        if(old != null)
//            workerMap.putIfAbsent(key, new Worker(host, port, old.capabilities()));
        String key = generateKey(host, port);
        Worker worker = workerMap.get(key);
        if(worker != null) {
            worker.updateLastSeen();
        }
    }

    /**
 * Marks a worker as dead by removing it from the registry.
 * This method effectively removes the worker entry, making it unavailable for task assignment.
 *
 * @param host The hostname or IP address of the worker to mark as dead.
 * @param port The port number of the worker to mark as dead.
 */
    public void markWorkerDead(String host, int port){
        removeWorker(host, port);
    }

    /**
 * Retrieves a list of workers that possess a specific capability.
 *
 * @param requiredSkill The capability string to filter workers by.
 * @return A {@link List} of {@link Worker} objects that have the specified skill.
 *         Returns an empty list if no workers with the capability are found.
 */
    public List<Worker> getWorkersByCapability(String requiredSkill){
        return getWorkers().stream()
                .filter(k -> k.capabilities().contains(requiredSkill))
                .collect(Collectors.toList());
    }

    /**
 * A private helper method to remove a worker from the registry based on its host and port.
 *
 * @param host The hostname or IP address of the worker to remove.
 * @param port The port number of the worker to remove.
 */
    private void removeWorker(String host, int port){
        workerMap.remove(generateKey(host, port));
    }
}

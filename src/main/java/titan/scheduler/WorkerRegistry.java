/*
Copyright 2026 Ram Narayanan

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

 */



package titan.scheduler;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class WorkerRegistry {
    private final Map<String, Worker> workerMap;

    public WorkerRegistry(){
        this.workerMap = new ConcurrentHashMap<>();
    }

    public Map<String, Worker> getWorkerMap(){
        return this.workerMap;
    }

    public String getWorkerKey(String host, int port){
        return generateKey(host, port);
    }

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

    public Collection<Worker> getWorkers(){
        return workerMap.values();
    }

    private String generateKey(String host, int port) {
        return host + ":" + port;
    }

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

    public void markWorkerDead(String host, int port){
        removeWorker(host, port);
    }

    public List<Worker> getWorkersByCapability(String requiredSkill){
        return getWorkers().stream()
                .filter(k -> k.capabilities().contains(requiredSkill))
                .collect(Collectors.toList());
    }

    private void removeWorker(String host, int port){
        workerMap.remove(generateKey(host, port));
    }
}

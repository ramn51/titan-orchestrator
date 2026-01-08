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

public class Job implements Comparable<Job> {
    public enum Status{
        PENDING, RUNNING, COMPLETED, FAILED, DEAD
    }

    public static final int PRIORITY_LOW = 0;
    public static final int PRIORITY_NORMAL = 1;
    public static final int PRIORITY_HIGH = 2;

    private String id;
    private String payload;
    private int retryCount;
    private Status status;

    private final int priority;
    private final long scheduledTime;

    private List<String> dependenciesIds = null;
    private Set<String> satisfiedDeps = null;

    private String preferredWorkerId;
    private boolean affinityRequired = false;

    public Job(String payload) {
        this(payload, PRIORITY_NORMAL, 0);
    }

    public Job(String payload, int priority, long delayInMs){
        this.payload = payload;
        this.id = UUID.randomUUID().toString();
        this.retryCount = 0;
        this.status = Status.PENDING;
        this.priority = priority;
        this.scheduledTime = System.currentTimeMillis() + delayInMs;
    }

    public Job(String id, String payload, int priority, long delayInMs, List<String> dependenciesIds){
        this.payload = payload;
        this.id = id;
        this.retryCount = 0;
        this.status = Status.PENDING;
        this.priority = priority;
        this.scheduledTime = System.currentTimeMillis() + delayInMs;

        if(dependenciesIds != null && !dependenciesIds.isEmpty()){
            this.dependenciesIds = dependenciesIds;
            this.satisfiedDeps = new HashSet<>();
        }
    }

    public void setId(String id){
        this.id = id;
    }

    public void setPayload(String payload){
        this.payload = payload;
    }

    public boolean isReady(){
        return dependenciesIds == null || satisfiedDeps.size() >= dependenciesIds.size();
    }

    public void resolveDependencies(String parentId){
        if(dependenciesIds!=null && dependenciesIds.contains(parentId)){
            satisfiedDeps.add(parentId);
        }
    }

    public static Job fromDagString(String jobStr) {
        String raw = jobStr.trim();
        boolean isAffinityRequired = false;
        try {
            // STRATEGY: Parse from the OUTSIDE IN to handle variable pipes in the payload.

            // STEP 0: Parse the Affinity or sticky flag first and then strip that part and proceed with previous normal string
            if (raw.endsWith("|AFFINITY") || raw.endsWith("|STICKY")) {
                isAffinityRequired = true;
                int lastPipe = raw.lastIndexOf('|');
                raw = raw.substring(0, lastPipe).trim(); // Strip the flag off
            }

            // --- STEP 1: RIGHT SIDE (Metadata) ---

            // 1. Extract Parents: "[JOB_A, JOB_B]" or "[]"
            int bracketStart = raw.lastIndexOf('[');
            if (bracketStart == -1) throw new IllegalArgumentException("Missing parent brackets []");
            String parentsStr = raw.substring(bracketStart);

            // Remove parents from string for next step
            // Current: ID|SKILL|PAYLOAD...|PRIORITY|DELAY|
            String temp = raw.substring(0, bracketStart).trim();
            if (temp.endsWith("|")) temp = temp.substring(0, temp.length() - 1); // Safety trim

            // 2. Extract Delay (Last number)
            int lastPipe = temp.lastIndexOf('|');
            long delay = Long.parseLong(temp.substring(lastPipe + 1).trim());
            temp = temp.substring(0, lastPipe);

            // 3. Extract Priority (Next number from right)
            lastPipe = temp.lastIndexOf('|');
            int priority = Integer.parseInt(temp.substring(lastPipe + 1).trim());
            temp = temp.substring(0, lastPipe);

            // --- STEP 2: LEFT SIDE (Identification) ---

            // 4. Extract ID (First item)
            int firstPipe = temp.indexOf('|');
            String id = temp.substring(0, firstPipe).trim();
            temp = temp.substring(firstPipe + 1);

            // 5. Extract Skill (Second item)
            int secondPipe = temp.indexOf('|');
            String skill = temp.substring(0, secondPipe).trim();

            // --- STEP 3: MIDDLE (The Payload) ---

            // 6. The Remainder is the Payload (e.g., "RUN_PAYLOAD|calc.py|BASE64...")
            String finalPayload = temp.substring(secondPipe + 1).trim();

            // --- STEP 4: CONSTRUCT JOB ---
            // 1. Auto-Prefix the ID (if not already present)
            // This ensures "GEN_LOGIC" becomes "DAG-GEN_LOGIC"
            String dagId = id.startsWith("DAG-") ? id : "DAG-" + id;

            // Parse dependencies string "[A,B]" -> List<String>
            // 2. Auto-Prefix the Dependencies
            // IMPORTANT: If Job A is renamed "DAG-A", Job B must depend on "DAG-A", not "A".
            String cleanParents = parentsStr.replace("[", "").replace("]", "").trim();
            java.util.List<String> deps = new java.util.ArrayList<>();
            if (!cleanParents.isEmpty()) {
                for (String p : cleanParents.split(",")) {
                    String cleanP = p.trim();
                    if(!cleanP.isEmpty()){
                        // Prefix the dependency ID too
                        deps.add(cleanP.startsWith("DAG-") ? cleanP : "DAG-" + cleanP);
                    }
                }
//                for (String p : cleanParents.split(",")) {
//                    deps.add(p.trim());
//                }
            }

//            Job job = new Job(id, finalPayload, priority, delay, deps);
            String payloadWithId = finalPayload + "|" + dagId;
            Job job = new Job(dagId, skill + "|" + payloadWithId, priority, delay, deps);
            job.setAffinityRequired(isAffinityRequired);
//            job.setRequiredSkill(skill);
            return job;

        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse DAG Job: " + jobStr + " [" + e.getMessage() + "]");
        }
    }

    // Helper method for the File I/O logic
    private static String constructFilePayload(String type, String filename, String port) {
        try {
            java.io.File file = new java.io.File("perm_files" + java.io.File.separator + filename);
            String base64Content = "";

            if (file.exists()) {
                byte[] fileBytes = java.nio.file.Files.readAllBytes(file.toPath());
                base64Content = java.util.Base64.getEncoder().encodeToString(fileBytes);
            } else {
                System.err.println("[ERROR] Warning: File not found: " + filename);
                // Fallback: send without base64, though worker might fail
                return type + "|" + filename;
            }

            if (type.equalsIgnoreCase("RUN")) {
                return "RUN_PAYLOAD|" + filename + "|" + base64Content;
            } else { // DEPLOY
                return "DEPLOY_PAYLOAD|" + filename + "|" + base64Content + "|" + port;
            }
        } catch (Exception e) {
            System.err.println("[FAIL] Payload Error: " + e.getMessage());
            return type + "|" + filename;
        }
    }

    public List<String> getDependenciesIds(){
        if(dependenciesIds == null)
            return Collections.emptyList();
        else
            return dependenciesIds;
    }

    public long getScheduledTime() { return scheduledTime; }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }

    public void incrementRetry() {
        this.retryCount++;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public String getPayload() {
        return payload;
    }

    public String getId() {
        return id;
    }

    @Override
    public int compareTo(Job other) {
        return Integer.compare(other.priority, this.priority);
    }

    @Override
    public String toString() {
        return String.format("[%s] Job %s (Retries: %d)", status, id, retryCount);
    }

    public void setPreferredWorkerId(String preferredWorkerId){this.preferredWorkerId = preferredWorkerId;}

    public String getPreferredWorkerId(){return this.preferredWorkerId;}

    public void setAffinityRequired(boolean isAffinityRequired){this.affinityRequired=isAffinityRequired;}
    public Boolean isAffinityRequired(){return this.affinityRequired;}

}

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

// This will hold the task execution status (useful for the dashboard info and logs).
class TaskExecution {
    String jobId;
    Worker assignedWorker;
    long startTime;
    long endTime;
    Job.Status status;
    String output; // To store "RESULT: 5050" or error logs

    public TaskExecution(String jobId, Worker worker) {
        this.jobId = jobId;
        this.assignedWorker = worker;
        this.startTime = System.currentTimeMillis();
        this.status = Job.Status.RUNNING;
    }

    public void complete(String output) {
        this.status = Job.Status.COMPLETED;
        this.endTime = System.currentTimeMillis();
        this.output = output;
    }

    public void fail(String error) {
        this.status = Job.Status.FAILED;
        this.endTime = System.currentTimeMillis();
        this.output = error;
    }

    public long getDuration() {
        if (endTime == 0) return System.currentTimeMillis() - startTime;
        return endTime - startTime;
    }
}
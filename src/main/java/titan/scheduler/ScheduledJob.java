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

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class ScheduledJob implements Delayed {
    private final Job job;
    public ScheduledJob(Job job){
        this.job = job;
    }

    public Job getJob() {
        return job;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long diff = job.getScheduledTime() - System.currentTimeMillis();
        return unit.convert(diff, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        return Long.compare(this.getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
    }
}

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

/**
 * Represents a job that is scheduled to run at a future time.
 * This class wraps a {@link Job} and implements the {@link Delayed} interface,
 * allowing it to be used with {@link java.util.concurrent.DelayQueue} for time-based scheduling.
 * The delay is determined by the {@link Job#getScheduledTime()} property.
 */
    public class ScheduledJob implements Delayed {
    private final Job job;
    /**
 * Constructs a new {@code ScheduledJob} with the specified job.
 * The scheduling time for this job is derived from the provided {@link Job} object.
 *
 * @param job The {@link Job} to be scheduled. Must not be {@code null}.
 */
    public ScheduledJob(Job job){
        this.job = job;
    }

    /**
 * Returns the underlying {@link Job} instance associated with this scheduled job.
 *
 * @return The {@link Job} that this {@code ScheduledJob} is wrapping.
 */
    public Job getJob() {
        return job;
    }

    @Override
    /**
 * Returns the remaining delay associated with this object, in the given time unit.
 * The delay is calculated as the difference between the job's scheduled time
 * and the current system time.
 *
 * @param unit The time unit in which to return the delay.
 * @return The remaining delay; a negative value indicates that the delay has already expired.
 * @see java.util.concurrent.Delayed#getDelay(TimeUnit)
 */
    public long getDelay(TimeUnit unit) {
        long diff = job.getScheduledTime() - System.currentTimeMillis();
        return unit.convert(diff, TimeUnit.MILLISECONDS);
    }

    @Override
    /**
 * Compares this object with the specified object for order.
 * Returns a negative integer, zero, or a positive integer as this object is less than,
 * equal to, or greater than the specified object.
 * <p>
 * {@code ScheduledJob} instances are ordered primarily by their remaining delay.
 * A job with a shorter remaining delay (i.e., scheduled to run sooner) is considered "less than"
 * a job with a longer remaining delay.
 *
 * @param other The {@link Delayed} object to be compared.
 * @return A negative integer, zero, or a positive integer as this object is less than,
 *         equal to, or greater than the specified object.
 * @throws ClassCastException if the specified object's type prevents it from being compared to this object.
 * @see java.lang.Comparable#compareTo(Object)
 */
    public int compareTo(Delayed other) {
        return Long.compare(this.getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
    }
}

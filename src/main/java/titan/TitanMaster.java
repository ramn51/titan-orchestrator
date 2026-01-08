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

package titan;


import titan.scheduler.Scheduler;

public class TitanMaster {
    public static void main(String[] args) {
        // Start Scheduler on port 9090
        Scheduler scheduler = new Scheduler(9090);
        scheduler.start();

        scheduler.startAutoScaler();

        // Keep main thread alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
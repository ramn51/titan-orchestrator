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

/**
 * The main entry point for the Titan distributed system. This class is responsible
 * for initializing and orchestrating core components such as the scheduler and
 * autoscaler, and for managing the overall lifecycle of the Titan master node.
 * It sets up the necessary services and keeps the application running.
 */
    public class TitanMaster {
    /**
 * The main method and entry point for the TitanMaster application.
 * This method initializes and starts the core services required for the Titan
 * master node, including the scheduler and the autoscaler. It also retrieves
 * critical configuration parameters from {@link TitanConfig} for services like Redis
 * and worker heartbeat intervals.
 * <p>
 * The main thread is kept alive indefinitely to ensure the application continues
 * to run and manage its services.
 *
 * @param args Command line arguments (not currently used).
 */
    public static void main(String[] args) {
        // Start Scheduler on port 9090
        Scheduler scheduler = new Scheduler(9090);
        scheduler.start();

        scheduler.startAutoScaler();

        String redisHost = TitanConfig.get("titan.redis.host", "localhost");
        int redisPort = TitanConfig.getInt("titan.redis.port", 6379);
        int heartBeat = TitanConfig.getInt("titan.worker.heartbeat.interval", 10);

        // Keep main thread alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
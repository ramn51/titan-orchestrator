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

package titan.manual.storage;

import titan.storage.TitanJRedisAdapter;

import java.util.Set;
import titan.TitanConfig;

public class TitanJRedisAdapterTest {
    public static void main(String[] args) {
        String redisHost = TitanConfig.get("titan.redis.host", "localhost");
        int redisPort = TitanConfig.getInt("titan.redis.port", 6379);
        int heartBeat = TitanConfig.getInt("titan.worker.heartbeat.interval", 10);

        try (TitanJRedisAdapter redis = new TitanJRedisAdapter(redisHost, redisPort)) {
            System.out.println("Connecting to RedisJava...");
            redis.connect();

            System.out.println("Testing SET...");
            String setResp = redis.set("titan:status", "ONLINE");
            System.out.println("SET Response: " + setResp); // Should be "+OK"

            System.out.println("Testing GET...");
            String getResp = redis.get("titan:status");
            System.out.println("GET Response: " + getResp); // Should be "ONLINE"

            redis.set("titan:status", "BUSY");
            System.out.println("Updated Status: " + redis.get("titan:status"));

            // --- 2. Set Tests (New) ---
            System.out.println("\n--- Testing Set Operations ---");
            String workersKey = "titan:active_workers";

            // Test SADD
            System.out.println("Testing SADD (Adding Worker 1)...");
            long saddResp1 = redis.sadd(workersKey, "10.0.0.1:8080");
            System.out.println("SADD Response: " + saddResp1); // Should be 1 (New)

            System.out.println("Testing SADD (Adding Worker 2)...");
            long saddResp2 = redis.sadd(workersKey, "10.0.0.2:9090");
            System.out.println("SADD Response: " + saddResp2); // Should be 1 (New)

            System.out.println("Testing SADD (Duplicate Worker 1)...");
            long saddResp3 = redis.sadd(workersKey, "10.0.0.1:8080");
            System.out.println("SADD Response: " + saddResp3); // Should be 0

            // Test SMEMBERS
            System.out.println("Testing SMEMBERS...");
            Set<String> members = redis.smembers(workersKey);
            System.out.println("Active Workers found: " + members.size());
            for (String member : members) {
                System.out.println(" - " + member);
            }

            if (members.contains("10.0.0.1:8080") && members.contains("10.0.0.2:9090")) {
                System.out.println("[PASS] All Set Tests Passed!");
            } else {
                System.err.println("[ERROR]Set Content Verification Failed!");
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("[ERROR] Connection Failed! Is RedisJava running?");
        }
    }
}
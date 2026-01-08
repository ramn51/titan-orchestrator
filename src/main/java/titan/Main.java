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

import titan.network.RpcWorkerServer;
import titan.scheduler.Scheduler;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            return;
        }

        String mode = args[0].toUpperCase();

        try {
            switch (mode) {
                case "SCHEDULER":
                    int schedPort = (args.length > 1) ? Integer.parseInt(args[1]) : 9090;
                    System.out.println("[INFO] Starting Titan Scheduler on port " + schedPort);
                    Scheduler scheduler = new Scheduler(schedPort);
                    scheduler.start();
                    break;

                case "WORKER":
                    // Usage: WORKER <MY_PORT> <SCHEDULER_HOST> <SCHEDULER_PORT> <CAPABILITY>
                    int myPort = (args.length > 1) ? Integer.parseInt(args[1]) : 8081;
                    String sHost = (args.length > 2) ? args[2] : "localhost";
                    int sPort = (args.length > 3) ? Integer.parseInt(args[3]) : 9090;
                    String cap = (args.length > 4) ? args[4] : "GENERAL";
                    boolean isPermanent = args.length > 5 && Boolean.parseBoolean(args[5]);

                    System.out.println("[INFO] Starting Titan Worker on port " + myPort);
                    RpcWorkerServer worker = new RpcWorkerServer(myPort, sHost, sPort, cap, isPermanent);
                    worker.start();
                    break;

                default:
                    System.out.println("Unknown mode: " + mode);
                    printUsage();
                    break;
            }
        } catch (Exception e) {
            System.err.println("[FAIL] Critical failure during startup:");
            e.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  java -jar Titan.jar SCHEDULER [port]");
        System.out.println("  java -jar Titan.jar WORKER [myPort] [schedHost] [schedPort] [capability]");
    }
}
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
/**
 * The main entry point for the Titan distributed system application.
 * This class allows the application to be started in one of two primary modes:
 * <ul>
 *     <li><b>SCHEDULER</b>: Runs the Titan Scheduler component, responsible for coordinating tasks and resources.</li>
 *     <li><b>WORKER</b>: Runs a Titan Worker component, which executes tasks assigned by the Scheduler.</li>
 * </ul>
 * The operational mode and associated parameters are determined by command-line arguments.
 */
    public class Main {
    /**
 * The main method, serving as the application's entry point.
 * It parses command-line arguments to determine whether to start the Titan application
 * as a Scheduler or a Worker, and initializes the respective components with the provided parameters.
 * If no arguments are provided or an unknown mode is specified, it prints usage instructions.
 *
 * @param args Command-line arguments specifying the operational mode and configuration parameters.
 *             Expected formats:
 *             <ul>
 *                 <li><code>SCHEDULER [port]</code>: Starts the Scheduler. `port` is optional, defaults to 9090.</li>
 *                 <li><code>WORKER [myPort] [schedHost] [schedPort] [capability] [isPermanent]</code>: Starts a Worker.
 *                     `myPort` is optional, defaults to 8081.
 *                     `schedHost` is optional, defaults to "localhost".
 *                     `schedPort` is optional, defaults to 9090.
 *                     `capability` is optional, defaults to "GENERAL".
 *                     `isPermanent` is optional, defaults to false, indicating if the worker should re-register on disconnect.
 *                 </li>
 *             </ul>
 * Catches and prints any critical exceptions that occur during the startup
 *                   and initialization of the Scheduler or Worker components.
 */
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

    /**
 * Prints the correct command-line usage instructions for running the Titan application.
 * This method is called when no arguments are provided or an invalid mode is specified,
 * guiding the user on how to start the application in SCHEDULER or WORKER mode with their respective parameters.
 */
    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  java -jar Titan.jar SCHEDULER [port]");
        System.out.println("  java -jar Titan.jar WORKER [myPort] [schedHost] [schedPort] [capability]");
    }
}
package network;

import scheduler.TaskHandler;
import scheduler.tasks.FileHandler;
import scheduler.tasks.PdfConversionHandler;
import scheduler.tasks.ScriptExecutorHandler;
import scheduler.tasks.ServiceHandler;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class RpcWorkerServer {
    private int port;
    private final ExecutorService threadPool;
    private volatile boolean isRunning = true;
    private String capability;
    private int schedulerPort;
    private String schedulerHost;
    private Map<String, TaskHandler> taskHanlderMap;

    private static final int MAX_THREADS = 4;
    private final ExecutorService workerPool;
    private final AtomicInteger activeJobs;

    public RpcWorkerServer( int myPort, String schedulerHost, int schedulerPort, String capability){
        this.port = myPort;
        this.threadPool = Executors.newCachedThreadPool();
        this.capability = capability;
        this.schedulerHost = schedulerHost;
        this.schedulerPort = schedulerPort;
        this.taskHanlderMap = new HashMap<>();

        workerPool = Executors.newFixedThreadPool(MAX_THREADS);
        activeJobs =  new AtomicInteger(0);

        addTaskHandler();
    }

    public void addTaskHandler(){
        taskHanlderMap.put("PDF_CONVERT", new PdfConversionHandler());
        taskHanlderMap.put("STAGE_FILE", new FileHandler());
        taskHanlderMap.put("START_SERVICE", new ServiceHandler("START", this));
        taskHanlderMap.put("STOP_SERVICE", new ServiceHandler("STOP", this));
        taskHanlderMap.put("RUN_SCRIPT", new ScriptExecutorHandler());

        taskHanlderMap.put("DEPLOY_PAYLOAD", new FileHandler());

        // This is for Shutting down the worker
        taskHanlderMap.put("SHUTDOWN_WORKER", (payload) -> {
            new Thread(() -> {
                try { Thread.sleep(1000); } catch (Exception e) {}
                System.out.println("Received SHUTDOWN command. Exiting...");
                System.exit(0);
            }).start();
            return "ACK_SHUTTING_DOWN";
        });
    }

    public void start() throws Exception {
        System.out.println("DEBUG: Attempting to bind to port: " + this.port);

        try(ServerSocket serverSocket = new ServerSocket(port)){
            System.out.println("Worker Server started on port " + port);
            registerWithScheduler();

            while(this.isRunning){
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(() -> clientHandler(clientSocket));
            }

        } catch (Exception e){
            e.printStackTrace();
        }
    }

    private void registerWithScheduler() throws Exception {
        try(Socket socket = new Socket(schedulerHost, schedulerPort);
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream())){
            String requestPayload = port + "||" + capability;
            TitanProtocol.send(out, TitanProtocol.OP_REGISTER, requestPayload);
            TitanProtocol.TitanPacket responsePacket = TitanProtocol.read(in);
            if ("REGISTERED".equals(responsePacket.payload)) {
                System.out.println("[OK] Successfully registered with Scheduler!");
            } else {
                System.err.println("[FAIL] Registration failed: " + responsePacket.payload);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    private void clientHandler(Socket socket){
        try(socket; DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        ){
            while (!socket.isClosed()) {
                try {
                    TitanProtocol.TitanPacket packet = TitanProtocol.read(in);

                    if (packet.opCode == TitanProtocol.OP_HEARTBEAT) {
                        String stats = "PONG|" + activeJobs.get() + "|" + MAX_THREADS;
                        TitanProtocol.send(out, TitanProtocol.OP_ACK, stats);

                    } else if (packet.opCode == TitanProtocol.OP_STAGE) {
                        // handleExecution(out, packet.payload, "STAGE_FILE");
                        handleSyncExecution(out, packet.payload, "STAGE_FILE");
                    } else if (packet.opCode == TitanProtocol.OP_START_SERVICE) {
//                        handleExecution(out, packet.payload, "START_SERVICE");
                        handleSyncExecution(out, packet.payload, "START_SERVICE");
                    } else if (packet.opCode == TitanProtocol.OP_STOP) {
//                        handleExecution(out, packet.payload, "STOP_SERVICE");
                        handleSyncExecution(out, packet.payload, "STOP_SERVICE");
                    } else if (packet.opCode == TitanProtocol.OP_RUN) {
                        if (packet.payload.startsWith("SHUTDOWN_WORKER")) {
                            System.out.println("Worker received kill signal. Shutting down...");
                            // Send confirmation back before dying
                            TitanProtocol.send(out, TitanProtocol.OP_RUN, "SUCCESS: Worker shutting down.");
                            Thread.sleep(100); // add busy waiting for the OS to handle the exit
                            System.exit(0);
                        }

//                        handleExecution(out, packet.payload, "RUN_SCRIPT");
                        handleAsyncExecution(out, packet.payload);
                    }
                } catch (EOFException e) {
                    System.out.println("Scheduler disconnected.");
                    break;
                } catch (Exception e) {
                    System.err.println("Error processing packet: " + e.getMessage());
                    break;
                }
            }
        } catch (IOException e){
            System.err.println("Connection error in clientHandler: " + e.getMessage());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void handleExecution(DataOutputStream out, String payload, String forceTaskType) {
        if(activeJobs.get() >= MAX_THREADS){
            try {
                TitanProtocol.send(out, TitanProtocol.OP_ERROR, "ERROR_WORKER_SATURATED");
            } catch (IOException e) { e.printStackTrace(); }
        } else {
            activeJobs.incrementAndGet();
            try {
                workerPool.submit(() -> {
                    try {
                        // If forceTaskType is set (for DEPLOY), use it. Otherwise parse from payload.
                        String response = (forceTaskType != null)
                                ? processCommandExplicit(forceTaskType, payload)
                                : processCommand(payload);

                        byte status = response.startsWith("ERROR") || response.contains("FAILED")
                                ? TitanProtocol.OP_ERROR
                                : TitanProtocol.OP_ACK;

                        synchronized (out) {
                            try {
                                TitanProtocol.send(out, status, response);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("Worker execution error: " + e.getMessage());
                    } finally {
                        activeJobs.decrementAndGet();
                    }
                }).get();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    private String processCommandExplicit(String taskType, String taskData) {
        TaskHandler handler = taskHanlderMap.get(taskType);
        if(handler!=null){
            try{
                return handler.execute(taskData);
            } catch (Exception e){
                return "JOB_FAILED " + e.getMessage();
            }
        }
        return "ERROR: Unknown TaskType " + taskType;
    }

    public void notifyMasterOfServiceStop(String serviceId) {
        // masterHost and masterPort should be variables in your RpcWorkerServer class
        try (Socket socket = new Socket(this.schedulerHost, this.schedulerPort);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {
            TitanProtocol.send(out, TitanProtocol.OP_UNREGISTER_SERVICE, serviceId);
            System.out.println("[TitanProtocol] Sent UNREGISTER_SERVICE for " + serviceId);

        } catch (IOException e) {
            System.err.println("[FAIL] Failed to notify Master: " + e.getMessage());
        }
    }

    private String processCommand(String payload){
            // Payload format: "TASK_TYPE|ARGS..."
            String[] parts = payload.split("\\|", 2);
            if(parts.length < 2)
                return "INVALID_JOB_FORMAT";

            // Ex: "START_SERVICE" or "PDF_CONVERT"
            String taskType = parts[0];
            // Ex: "file.jar|jobId|8085"
            String taskData = parts[1];

            if (payload.contains("SLEEP")) {
                try { Thread.sleep(5000); } catch (Exception e) {}
            }

            if (taskType.equals("TEST")) {
                return "SUCCESS_PROCESSED_" + payload;
            }

            if (payload.contains("FAIL")) {
                return "JOB_FAILED_SIMULATED_ERROR";
            }

            TaskHandler handler = taskHanlderMap.get(taskType);
            if(handler!=null){
                try{
                   return handler.execute(taskData);
                } catch (Exception e){
                    return "JOB_FAILED" + e.getMessage();
                }
            } else{
                return "ERROR: Task doesnt exist so I dont know how to do " + taskType;
            }
    }

    private void handleAsyncExecution(DataOutputStream out, String payload){
        if(activeJobs.get() >= MAX_THREADS){
            try{
                TitanProtocol.send(out, TitanProtocol.OP_ERROR, "ERROR_WORKER_SATURATED");
            } catch (IOException e){
                e.printStackTrace();
            }
            return;
        }

        activeJobs.incrementAndGet();
        workerPool.submit(() ->{
            // Parse Job ID for Callback
            // Payload Format expected: "JOB-123|calc.py" (Standard) or "calc.py" (Legacy/Direct)
            String[] parts = payload.split("\\|", 2);
            String jobId = (parts.length > 1) ? parts[0] : "UNKNOWN";
            String taskData = (parts.length > 1) ? parts[1] : payload;
            // This for Test jobs
            if(parts[0].equals("TEST")) { jobId = "TEST-JOB"; taskData = parts[1]; }
            try {
                System.out.println("[ASYNC] Starting job "+ jobId + ": " + taskData);
                TaskHandler handler = taskHanlderMap.get("RUN_SCRIPT");
                String result = handler.execute(taskData);
                // This allows the worker to handle PDF_CONVERT or any other key
//                String result = processCommand(taskData);

                System.out.println("[ASYNC] Finished "+ jobId);
                sendCallback(jobId, "COMPLETED", result);
            } catch(Exception e){
                System.err.println("[ASYNC] Failed " + jobId + ": " + e.getMessage());
                sendCallback(jobId, "FAILED", e.getMessage());
            } finally {
                activeJobs.decrementAndGet();
            }
        });
        try {
            TitanProtocol.send(out, TitanProtocol.OP_ACK, "JOB_ACCEPTED");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void sendCallback(String jobId, String status, String result) {
        if(jobId.equals("UNKNOWN") || jobId.equals("TEST-JOB")) return;

        try (Socket socket = new Socket(schedulerHost, schedulerPort);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            String msg = jobId + "|" + status + "|" + result;
            TitanProtocol.send(out, TitanProtocol.OP_JOB_COMPLETE, msg);
            TitanProtocol.read(in);

        } catch (Exception e) {
            System.err.println("[FAIL] Callback failed: " + e.getMessage());
        }
    }

    private void handleSyncExecution(DataOutputStream out, String payload, String forceTaskType){
        try{
            String response = processCommandExplicit(forceTaskType, payload);
            byte status = response.startsWith("ERROR") || response.contains("FAILED")
                    ? TitanProtocol.OP_ERROR
                    : TitanProtocol.OP_ACK;
            TitanProtocol.send(out, status, response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stop(){
        isRunning = false;
        threadPool.shutdown();
    }

    public static void main(String[] args) throws Exception {
        int myPort = 8080;
        String schedHost = "localhost";
        int schedPort = 9090;
        String capability = "GENERAL";

        // 2. Parse Arguments (Order: port, schedHost, schedPort, capability)
        if (args.length > 0) {
            try {
                myPort = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port provided, using default 8080");
            }
        }

        if (args.length > 1) schedHost = args[1];

        if (args.length > 2) {
            try {
                schedPort = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid scheduler port, using default 9090");
            }
        }

        if (args.length > 3) capability = args[3];

        System.out.println("Starting Worker Server...");
        System.out.println("Local Port: " + myPort);
        System.out.println("Target Scheduler: " + schedHost + ":" + schedPort);
        System.out.println("Capability: " + capability);

        RpcWorkerServer rpcWorkerServer = new RpcWorkerServer(myPort, schedHost, schedPort, capability);
        rpcWorkerServer.start();
    }
}

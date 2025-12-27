package network;

import scheduler.Job;
import scheduler.Scheduler;
import scheduler.WorkerRegistry;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import network.TitanProtocol.TitanPacket;

public class SchedulerServer {
    private final int port;
    private boolean isRunning = true;
    private final ExecutorService threadPool;
    Scheduler scheduler;
    private final ServerSocket serverSocket;

    private static final String PERM_FILES_DIR = "perm_files";

    public SchedulerServer(int port, Scheduler scheduler) throws IOException {
        this.port = port;
        threadPool = Executors.newCachedThreadPool();
        this.scheduler = scheduler;
        this.serverSocket = new ServerSocket(this.port);
    }

    public void start(){
        try(serverSocket){
            System.out.println("[OK] SchedulerServer Listening on port " + port);
            while(isRunning){
                try{
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Incoming connection from " + clientSocket.getInetAddress() + " Port" + clientSocket.getPort());
                    threadPool.submit(() -> clientHandler(clientSocket));
                } catch (IOException e){
                    e.printStackTrace();
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private String handleRegistration(Socket socket, String request){
            String[] parts = request.split("\\|\\|");
            if (parts.length < 1) return "ERROR_INVALID_REGISTRATION";
            int workerPort = Integer.parseInt(parts[0]);

            String capability = (parts.length > 1) ? parts[1] : "GENERAL";
            String host = socket.getInetAddress().getHostAddress();

            System.out.println("Registering Worker: " + host + " with " + capability);
            scheduler.registerWorker(host, workerPort, capability);
//            scheduler.getWorkerRegistry().addWorker(host, workerPort, capability);
            return ("REGISTERED");
    }

    public void clientHandler(Socket socket){
        try(socket;
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream())
        ){

            TitanPacket packet = TitanProtocol.read(in);
            String responsePayload;
            byte responseOpCode = TitanProtocol.OP_ACK;
            try {
                switch (packet.opCode) {
                    case TitanProtocol.OP_REGISTER:
                        responsePayload = handleRegistration(socket, packet.payload);
                        break;
                    default:
                        responsePayload = processCommand(packet);
                        break;
                }

//                if (responsePayload.startsWith("ERROR") || responsePayload.startsWith("UNKNOWN")) {
//                    responseOpCode = TitanProtocol.OP_ERROR;
//                }
            } catch (Throwable t) {
                t.printStackTrace();
                responsePayload = "SERVER_ERROR: " + t.getMessage();
                responseOpCode = TitanProtocol.OP_ERROR;
            }

            TitanProtocol.send(out, responseOpCode, responsePayload);

        } catch (IOException e) {
            System.err.println("Client Disconnected abruptly : " + e.getMessage());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String handleDeployRequest(String fileName, String port) {
        try {
            File file = new File(PERM_FILES_DIR + File.separator + fileName);
            if (!file.exists()) {
                return "ERROR: File not found in " + PERM_FILES_DIR;
            }

            byte[] fileBytes = Files.readAllBytes(file.toPath());
            String base64Content = Base64.getEncoder().encodeToString(fileBytes);

            Job job = new Job("TEMP_PAYLOAD", 1, 0);
            String internalId = job.getId();

            if (port == null || port.trim().isEmpty()) {
                port = "8085"; // Default if not specified
            }

            // If it's a Worker, we include the port in the ID for easy matching later
            String taggedId = (fileName.equalsIgnoreCase("Worker.jar"))
                    ? "WRK-" + port + "-" + internalId
                    : "TSK-" + internalId;

            // 3. Update the Job with the tagged ID and final payload
            job.setId(taggedId);

            // 2. Construct a Payload that contains EVERYTHING the worker needs
            // Format: DEPLOY_PAYLOAD|filename|base64data
            String payload = "DEPLOY_PAYLOAD|" + fileName + "|" + base64Content + "|" + port;
            job.setPayload(payload);
            scheduler.submitJob(job);

            System.out.println("[INFO] DEPLOY Job queued for file: " + fileName);
            return "DEPLOY_QUEUED";

        } catch (IOException e) {
            e.printStackTrace();
            return "ERROR: Server File IO issue";
        }
    }


    private String handleRunScript(String fileName){
        try {
            File file = new File(PERM_FILES_DIR + File.separator + fileName);
            if (!file.exists()) {
                return "ERROR: File not found in " + PERM_FILES_DIR;
            }
            byte[] fileBytes = Files.readAllBytes(file.toPath());
            String base64Content = Base64.getEncoder().encodeToString(fileBytes);

            if (base64Content == null) return "ERROR: File not found";

            String payload = "RUN_PAYLOAD|" + fileName + "|" + base64Content;
            // Payload: RUN_PAYLOAD|filename|base64
            Job job = new Job(payload, 1, 0);
            scheduler.submitJob(job);
            return "JOB_QUEUED";
        } catch (IOException e) {
            e.printStackTrace();
            return "ERROR: Server File IO issue";
        }
    }

    private void parseAndSubmitDAG(String request){
        String [] jobs = request.split(";");
        for(String jobDef: jobs){
            if(jobDef.trim().isEmpty()) continue;
            try {
                Job job = Job.fromDagString(jobDef.trim());
                System.out.println("[INFO] [PARSER] Created Job: " + job.getId());
                scheduler.submitJob(job);
            } catch (Exception e) {
                System.err.println("[FAIL] Failed to parse DAG job: " + jobDef + " Error: " + e.getMessage());
            }
        }
    }


    private String processCommand(TitanPacket packet){
        String payload = packet.payload;
        String[] parts;

        switch (packet.opCode){
            case TitanProtocol.OP_DEPLOY:
                parts = payload.split("\\|");
                if (parts.length < 1) return "ERROR: Missing filename";
                String deployFile = parts[0];
                String deployPort = (parts.length > 1) ? parts[1] : "";
                return handleDeployRequest(deployFile, deployPort);

            case TitanProtocol.OP_RUN:
                if (payload.isEmpty()) return "ERROR: Missing filename";
                return handleRunScript(payload);

            case TitanProtocol.OP_STATS_JSON:
                System.out.println("[INFO] Generating JSON Stats...");
                return scheduler.getSystemStatsJSON();

            case TitanProtocol.OP_CLEAN_STATS:
                scheduler.getLiveServiceMap().clear();
                return "Stats Map Cleared. Run STATS again to see fresh state.";

            case TitanProtocol.OP_UNREGISTER_SERVICE:
                String serviceId = payload;
                scheduler.getLiveServiceMap().remove(serviceId);
                System.out.println("[INFO] Cleaned up service: " + serviceId);
                return "ACK_UNREGISTERED";

            case TitanProtocol.OP_STOP:
                if (payload.isEmpty()) return "ERROR: Missing Service ID";
                return scheduler.stopRemoteService(payload);

            case TitanProtocol.OP_STATS:
                return scheduler.getSystemStats();

            case TitanProtocol.OP_SUBMIT_DAG:
                parseAndSubmitDAG(payload);
                return "DAG_ACCEPTED";

            case TitanProtocol.OP_SUBMIT_JOB:
                scheduler.submitJob(payload);
                return "JOB_ACCEPTED";


            default:
                return "UNKNOWN_OPCODE: " + packet.opCode;

        }
    }

    public void stop(){
        isRunning = false;
        threadPool.shutdown();

        try{
            if(serverSocket != null && !serverSocket.isClosed())
                serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

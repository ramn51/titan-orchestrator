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
        taskHanlderMap.put("START_SERVICE", new ServiceHandler("START"));
        taskHanlderMap.put("STOP_SERVICE", new ServiceHandler("STOP"));
        taskHanlderMap.put("RUN_SCRIPT", new ScriptExecutorHandler());
    }

    public void start() throws Exception {
        registerWithScheduler();
        try(ServerSocket serverSocket = new ServerSocket(port)){
            System.out.println("Worker Server started on port " + port);

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
            String requestPayload = "REGISTER||"+  port + "||" + capability;
            TitanProtocol.send(out, requestPayload);
            String response = TitanProtocol.read(in);
            if ("REGISTERED".equals(response)) {
                System.out.println("✅ Successfully registered with Scheduler!");
            } else {
                System.err.println("❌ Registration failed: " + response);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    private void clientHandler(Socket socket){
        try(socket; DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        ){
            String request = TitanProtocol.read(in);
            if (request == null) {
                return;
            }
            System.out.println("Received command: " + request);
            if (request.startsWith("PING")) {
                TitanProtocol.send(out, "PONG|" + activeJobs.get() + "|" + MAX_THREADS);
            }
            else if(request.startsWith("EXECUTE")){
                if(activeJobs.get() >= MAX_THREADS){
                    TitanProtocol.send(out, "ERROR_WORKER_SATURATED");
                } else{
                    activeJobs.incrementAndGet();
                    //.get() on the future to block THIS thread until the pool thread finishes
                    //This keeps the socket open while the work is happening
                    workerPool.submit(() ->{
                        try{
                            String response = processCommand(request);
                            synchronized (out) {
                                try {
                                    TitanProtocol.send(out, response);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        } catch (Exception e){
                            System.out.println("Some Erorr happened in the workerpool executor threads");
                            return;
                        } finally {
                            activeJobs.decrementAndGet();
                        }
                    }).get();
                }
            }
        } catch (IOException e){
            System.err.println("Error handling client: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String processCommand(String request){
        if(request.startsWith("EXECUTE")){
            // The request will be of the form EXECUTE PDF_CONVERT|fileName.docx
            String jobData = request.substring(8);
            String [] parts = jobData.split("\\|", 2);
            if(parts.length < 2)
                return "INVALID_JOB_FORMAT";

            String taskType = parts[0];
            String payload = parts[1];

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
                   return handler.execute(payload);
                } catch (Exception e){
                    return "JOB_FAILED" + e.getMessage();
                }
            } else{
                return "ERROR: Task doesnt exist so I dont know how to do " + taskType;
            }
        } else if(request.contains("PING")){
            return "PONG|" + activeJobs.get() + "|" + MAX_THREADS;
        } else {
            return "UNKNOWN_COMMAND";
        }
    }

    public void stop(){
        isRunning = false;
        threadPool.shutdown();
    }

    public static void main(String args[]) throws Exception{
        RpcWorkerServer rpcWorkerServer = new RpcWorkerServer(8080, "localhost", 9090, "GENERAL");
        rpcWorkerServer.start();
    }
}

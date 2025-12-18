package scheduler.tasks;

import scheduler.TaskHandler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceHandler implements TaskHandler {
    private static final Map<String, Process> runningServices = new ConcurrentHashMap<>();
    private static final String WORKSPACE_DIR = "./titan_workspace/";
    private final String operation;

    public ServiceHandler(String op){
        this.operation = op;
    }

    @Override
    public String execute(String payload) {
        String [] parts = payload.split("\\|");
        String fileName = parts[0];
        String serviceId = (parts.length > 1) ? parts[1] : "svc_" + System.currentTimeMillis();

        if(operation.equals("START")){
            return startProcess(fileName, serviceId);
        } else {
            return stopProcess(payload);
        }
    }

    private String startProcess(String fileName, String serviceId){
        // Payload: "filename|service_id"
        File scriptFile = new File(WORKSPACE_DIR, fileName);

        if(!scriptFile.exists()){
            return "ERROR: File not found at " + scriptFile.getAbsolutePath();
        }
        return launchDetachedProcess(serviceId, "python", scriptFile.getAbsolutePath());
    }

    private String launchDetachedProcess(String serviceId, String command, String scriptPath) {
        if (runningServices.containsKey(serviceId)) {
            return "SERVICE_ALREADY_RUNNING: " + serviceId;
        }

        try {

            ProcessBuilder pb = new ProcessBuilder(command, scriptPath);

            // 2. Separate Logs (Crucial for debugging background jobs)
            File logFile = new File(WORKSPACE_DIR, serviceId + ".log");
            pb.redirectOutput(logFile);
            pb.redirectError(logFile);

            // 3. START (Async/Detached)
            Process process = pb.start();

            // 4. Register in Memory Map
            runningServices.put(serviceId, process);

            // 5. Clean up Map when process dies
            process.onExit().thenRun(() -> {
                runningServices.remove(serviceId);
                System.out.println("🛑 Service Stopped: " + serviceId);
            });

            return "DEPLOYED_SUCCESS | ID: " + serviceId + " | PID: " + process.pid();

        } catch (IOException e) {
            e.printStackTrace();
            return "LAUNCH_ERROR: " + e.getMessage();
        }
    }

    private String stopProcess(String serviceId){
        Process p = runningServices.get(serviceId);
            if(p!=null){
                p.destroy();
                runningServices.remove(serviceId);
                return "STOPPED: " + serviceId;
            } else{
                return "UNKNOWN_SERVICE: " + serviceId;
            }
    }
}

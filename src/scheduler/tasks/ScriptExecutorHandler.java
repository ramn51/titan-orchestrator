package scheduler.tasks;

import scheduler.TaskHandler;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class ScriptExecutorHandler implements TaskHandler {
    private static final String WORKSPACE_DIR = "titan_workspace";
    @Override
    public String execute(String payload) {
        String filename = payload;

        System.out.println("⚡ [ScriptExecutor] Running: " + filename);
        File scriptFile = new File(WORKSPACE_DIR + File.separator + filename);

        if (!scriptFile.exists()) {
            return "ERROR: Script file not found: " + filename;
        }

        try {
            // 1. Determine Interpreter
            ProcessBuilder pb;
            if (filename.endsWith(".py")) {
                pb = new ProcessBuilder("python", scriptFile.getAbsolutePath());
            } else if (filename.endsWith(".sh")) {
                pb = new ProcessBuilder("/bin/bash", scriptFile.getAbsolutePath());
            } else {
                // Assume executable binary
                pb = new ProcessBuilder(scriptFile.getAbsolutePath());
            }

            // 2. Redirect stderr to stdout to capture errors
            pb.redirectErrorStream(true);

            // 3. Start Process
            Process process = pb.start();

            // 4. Wait for completion
            boolean finished = process.waitFor(60, TimeUnit.SECONDS);

            if (!finished) {
                process.destroy();
                return "ERROR: Script timed out (60s limit)";
            }

            // 5. Read Output (Byte-oriented for TitanProtocol compatibility)
            byte[] outputBytes = process.getInputStream().readAllBytes();
            String output = new String(outputBytes, StandardCharsets.UTF_8).trim();
            int exitCode = process.exitValue();

            // Format: COMPLETED|ExitCode|OutputContent
            return "COMPLETED|" + exitCode + "|" + output;

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR: Execution failed - " + e.getMessage();
        }
    }
}

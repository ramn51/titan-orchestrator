package titan.tasks;

import titan.network.RpcWorkerServer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Run with: java -cp <compiled classes> titan.tasks.ScriptExecutionStatusTest */
public final class ScriptExecutionStatusTest {
    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    private static void timeoutPolicy() {
        check(ScriptExecutorHandler.timeoutSeconds("DAG-etl") == 86400, "DAG timeout");
        check(ScriptExecutorHandler.timeoutSeconds("JOB-once") == 60, "standalone timeout");
        check(ScriptExecutorHandler.timeoutSeconds(null) == 60, "unknown timeout");
    }

    private static void callbackClassification() {
        check("COMPLETED".equals(ScriptOutcome.callbackStatus("COMPLETED|0|ok")), "zero exit");
        check("FAILED".equals(ScriptOutcome.callbackStatus("COMPLETED|1|error")), "nonzero exit");
        check("FAILED".equals(ScriptOutcome.callbackStatus("COMPLETED|7|REJECTED")), "approval rejection");
        check("FAILED".equals(ScriptOutcome.callbackStatus("ERROR: failed")), "execution error");
        check("FAILED".equals(ScriptOutcome.callbackStatus("COMPLETED|bad|output")), "malformed exit");
        check("FAILED".equals(ScriptOutcome.callbackStatus("COMPLETED|0")), "incomplete result");
        check("FAILED".equals(ScriptOutcome.callbackStatus(null)), "missing result");
    }

    private static void processExitToWorkerStatus() throws Exception {
        Path dir = Files.createTempDirectory("titan-script-status-");
        Path success = dir.resolve("success.py");
        Path rejection = dir.resolve("rejection.py");
        Files.writeString(success, "print('ok')\n");
        Files.writeString(rejection, "import sys\nprint('approval rejected')\nsys.exit(7)\n");
        RpcWorkerServer worker = new RpcWorkerServer(0, "127.0.0.1", 0, "GENERAL", true);
        try {
            ScriptExecutorHandler handler = new ScriptExecutorHandler(worker);
            String good = handler.execute("DAG-success|" + success);
            String denied = handler.execute("DAG-rejection|" + rejection);
            check(good.startsWith("COMPLETED|0|"), "successful script result");
            check("COMPLETED".equals(ScriptOutcome.callbackStatus(good)), "successful callback");
            check(denied.startsWith("COMPLETED|7|"), "rejection exit code preserved");
            check(denied.contains("approval rejected"), "rejection output preserved");
            check("FAILED".equals(ScriptOutcome.callbackStatus(denied)), "rejection callback");
            try {
                handler.execute("JOB-missing|" + dir.resolve("missing.py"));
                throw new AssertionError("missing script should fail");
            } catch (IllegalArgumentException expected) {
                check(expected.getMessage().contains("not found"), "missing script error");
            }
        } finally {
            worker.stop();
            Files.deleteIfExists(success);
            Files.deleteIfExists(rejection);
            Files.deleteIfExists(dir.resolve("DAG-success.log"));
            Files.deleteIfExists(dir.resolve("DAG-rejection.log"));
            Files.deleteIfExists(dir);
        }
    }

    @Test void dagAndStandaloneTimeouts() { timeoutPolicy(); }
    @Test void exitAndErrorClassification() { callbackClassification(); }
    @Test void realProcessExitCodes() throws Exception { processExitToWorkerStatus(); }

    @Test void activeScriptCanBeKilledByJobId() throws Exception {
        Path dir = Files.createTempDirectory("titan-cancel-script-");
        Path script = dir.resolve("slow.py");
        Path marker = dir.resolve("slow.started");
        Files.writeString(script, "from pathlib import Path\nimport time\nPath(__file__).with_suffix('.started').write_text('started')\ntime.sleep(30)\n");
        RpcWorkerServer worker = new RpcWorkerServer(0, "127.0.0.1", 0, "GENERAL", true);
        ScriptExecutorHandler handler = new ScriptExecutorHandler(worker);
        try {
            CompletableFuture<String> result = CompletableFuture.supplyAsync(() -> {
                try { return handler.execute("DAG-kill-test|" + script); }
                catch (Exception error) { return error.getMessage(); }
            });
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!Files.exists(marker) && System.nanoTime() < deadline) Thread.sleep(20);
            check(Files.exists(marker), "script reached running state");
            handler.cancel("DAG-kill-test");
            check(result.get(5, TimeUnit.SECONDS).contains("cancelled"), "killed script reports cancellation");
        } finally {
            worker.stop();
            Files.deleteIfExists(marker);
            Files.deleteIfExists(script);
            Files.deleteIfExists(dir.resolve("DAG-kill-test.log"));
            Files.deleteIfExists(dir);
        }
    }

    public static void main(String[] args) throws Exception {
        timeoutPolicy();
        callbackClassification();
        processExitToWorkerStatus();
        System.out.println("PASS ScriptExecutionStatusTest");
    }
}

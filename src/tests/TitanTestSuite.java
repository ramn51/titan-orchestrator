package tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class TitanTestSuite {

    // List of test classes to run
    private static final Class<?>[] TEST_CLASSES = {
            WorkerRegistrationTest.class,
            CronAndPriorityTest.class,
            DeployCommandIntegrationTest.class,
            RunScriptTest.class,
            WorkerScriptIntegrationTest.class,
            FaultToleranceTest.class,
            WorkerLoadTest.class,
            LoadBalancerTest.class,
            RpcWorkerTest.class,
            TitanDAGEndToEnd.class,
            EndToEndTest.class,
            // TestServer.class // Excluded: usually a utility/manual server, not an auto-test
    };

    public static void main(String[] args) {
        System.out.println("==========================================");
        System.out.println("🚀 TITAN MASTER INTEGRATION SUITE");
        System.out.println("==========================================");

        int passed = 0;
        int failed = 0;
        long startTime = System.currentTimeMillis();

        for (Class<?> testClass : TEST_CLASSES) {
            System.out.println("\n------------------------------------------");
            System.out.println("🏃 RUNNING: " + testClass.getSimpleName());
            System.out.println("------------------------------------------");

            boolean success = runTestInSeparateJvm(testClass);

            if (success) {
                System.out.println("✅ RESULT: PASS");
                passed++;
            } else {
                System.out.println("❌ RESULT: FAIL");
                failed++;
            }

            // Small cool-down to ensure OS releases ports (TCP TIME_WAIT)
            try { Thread.sleep(2000); } catch (InterruptedException e) {}
        }

        long duration = System.currentTimeMillis() - startTime;

        System.out.println("\n==========================================");
        System.out.println("📊 SUITE SUMMARY");
        System.out.println("==========================================");
        System.out.println("Total Tests: " + TEST_CLASSES.length);
        System.out.println("Passed:      " + passed);
        System.out.println("Failed:      " + failed);
        System.out.println("Duration:    " + (duration / 1000) + "s");

        if (failed > 0) {
            System.out.println("❌ SUITE FAILED");
            System.exit(1);
        } else {
            System.out.println("✅ SUITE PASSED");
            System.exit(0);
        }
    }

    private static boolean runTestInSeparateJvm(Class<?> clazz) {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = clazz.getName();

        ProcessBuilder builder = new ProcessBuilder(
                javaBin, "-cp", classpath, className
        );

        // Merge stderr so we see exceptions
        builder.redirectErrorStream(true);

        try {
            Process process = builder.start();

            // Stream output to console so we see what's happening live
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("   [TEST] " + line);
                }
            }

            int exitCode = process.waitFor();
            return exitCode == 0;

        } catch (Exception e) {
            System.err.println("❌ Error executing test process: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
}
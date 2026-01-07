package titan.manual;

import titan.network.RpcWorkerServer;
import titan.network.TitanProtocol;
import titan.scheduler.Scheduler;
import titan.scheduler.Job;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class TitanDAGEndToEnd {
    private static Scheduler scheduler;

    public static void main(String[] args) throws Exception {
        scheduler = new Scheduler(9090);
        scheduler.start();

        // WORKER 1 (Port 8080)
        RpcWorkerServer worker1 = new RpcWorkerServer(8080, "localhost", 9090, "TEST", false);
        new Thread(() -> { try { worker1.start(); } catch (Exception e) {} }).start();

        // WORKER 2 (Port 8081)
        RpcWorkerServer worker2 = new RpcWorkerServer(8081, "localhost", 9090, "TEST", false);
        new Thread(() -> { try { worker2.start(); } catch (Exception e) {} }).start();

        System.out.println("Waiting for workers to register...");
        Thread.sleep(3000);

        // --- TESTS ---
        testSimpleSequence();
        testDiamondDag();
        testCascadingFailure();
        testSimpleAffinity();
        testComplexFanOutAffinity();

        System.out.println("\n=== 🏁 ALL VALIDATIONS COMPLETE ===");
        System.exit(0);
    }

    private static void testSimpleSequence() throws Exception {
        System.out.println("\n--- 1. Validating Simple Sequence (A -> B) ---");
        // CHANGED: Use 'calc.py' instead of 'DataA'
        sendDag("S1_A|TEST|calc.py|2|0|[] ; S1_B|TEST|calc.py|1|0|[S1_A]");

        Thread.sleep(4000);

        assertStatus("S1_A", Job.Status.COMPLETED);
        assertStatus("S1_B", Job.Status.COMPLETED);
    }

    private static void testDiamondDag() throws Exception {
        System.out.println("\n--- 2. Validating Diamond DAG ---");
        // CHANGED: Use 'calc.py' for all nodes
        sendDag("D_ROOT|TEST|calc.py|2|0|[] ; " +
                "D_LEFT|TEST|calc.py|1|0|[D_ROOT] ; " +
                "D_RIGHT|TEST|calc.py|1|0|[D_ROOT] ; " +
                "D_FINAL|TEST|calc.py|1|0|[D_LEFT,D_RIGHT]");

        Thread.sleep(6000);

        assertStatus("D_ROOT", Job.Status.COMPLETED);
        assertStatus("D_LEFT", Job.Status.COMPLETED);
        assertStatus("D_RIGHT", Job.Status.COMPLETED);
        assertStatus("D_FINAL", Job.Status.COMPLETED);
    }

    private static void testCascadingFailure() throws Exception {
        System.out.println("\n--- 3. Validating Cascading Failure ---");
        // KEEP 'FAIL_THIS' because we WANT it to fail!
        sendDag("F_PARENT|TEST|FAIL_THIS|2|0|[] ; " +
                "F_CHILD|TEST|calc.py|1|0|[F_PARENT] ; " +
                "F_GRAND|TEST|calc.py|1|0|[F_CHILD]");

        Thread.sleep(12000); // Retries take time

        assertStatus("F_PARENT", Job.Status.DEAD);
        assertStatus("F_CHILD", Job.Status.DEAD);
        assertStatus("F_GRAND", Job.Status.DEAD);
    }

    private static void testSimpleAffinity() throws Exception {
        System.out.println("\n--- 4. Validating Simple Affinity (Parent -> Child) ---");
        // CHANGED: Use 'calc.py'
        String dag = "AFF_PARENT|TEST|calc.py|2|0|[] ; " +
                "AFF_CHILD|TEST|calc.py|1|0|[AFF_PARENT]|AFFINITY";

        sendDag(dag);

        Thread.sleep(5000);

        assertStatus("AFF_PARENT", Job.Status.COMPLETED);
        assertStatus("AFF_CHILD", Job.Status.COMPLETED);
        assertAffinity("AFF_PARENT", "AFF_CHILD");
    }

    private static void testComplexFanOutAffinity() throws Exception {
        System.out.println("\n--- 5. Validating Complex Fan-Out Affinity ---");
        // CHANGED: Use 'calc.py'
        String dag =
                "ML_TRAIN|TEST|calc.py|5|0|[] ; " +
                        "ML_EVAL_A|TEST|calc.py|1|0|[ML_TRAIN]|AFFINITY ; " +
                        "ML_EVAL_B|TEST|calc.py|1|0|[ML_TRAIN]|AFFINITY";

        sendDag(dag);

        Thread.sleep(6000);

        assertStatus("ML_TRAIN", Job.Status.COMPLETED);
        assertStatus("ML_EVAL_A", Job.Status.COMPLETED);
        assertStatus("ML_EVAL_B", Job.Status.COMPLETED);

        assertAffinity("ML_TRAIN", "ML_EVAL_A");
        assertAffinity("ML_TRAIN", "ML_EVAL_B");
    }

    // --- HELPERS (Keep exactly the same) ---

    private static void assertStatus(String id, Job.Status expected) throws InterruptedException {
        Job.Status actual = Job.Status.PENDING;
        for (int i = 0; i < 15; i++) {
            actual = scheduler.getJobStatus(id);
            if (actual == expected) break;
            Thread.sleep(500);
        }

        if (actual == expected) {
            System.out.println("[OK] [PASS] " + id + " is " + expected);
        } else {
            System.err.println("[FAIL] [FAIL] " + id + " expected " + expected + " but was " + actual);
        }
    }

    private static void assertAffinity(String parentId, String childId) {
        String jsonStats = scheduler.getSystemStatsJSON();
        String parentWorker = findWorkerForJob(jsonStats, parentId);
        String childWorker = findWorkerForJob(jsonStats, childId);

        if (parentWorker == null || childWorker == null) {
            System.err.println("[FAIL] Affinity Check: Could not find job history in JSON stats.");
            return;
        }

        if (parentWorker.equals(childWorker)) {
            System.out.println("[OK] [AFFINITY] " + childId + " followed " + parentId + " to Worker " + parentWorker);
        } else {
            System.err.println("[FAIL] [AFFINITY] Broken! Parent on " + parentWorker + " but Child on " + childWorker);
        }
    }

    private static String findWorkerForJob(String json, String jobId) {
        String[] workers = json.split("\"port\":");
        for (int i = 1; i < workers.length; i++) {
            String segment = workers[i];
            String port = segment.split(",")[0].trim();
            if (segment.contains("\"id\": \"" + jobId + "\"")) {
                return port;
            }
        }
        return null;
    }

    private static void sendDag(String dagPayload) {
        try (Socket s = new Socket("localhost", 9090);
             DataOutputStream out = new DataOutputStream(s.getOutputStream());
             DataInputStream in = new DataInputStream(s.getInputStream())
        ) {
            TitanProtocol.send(out, TitanProtocol.OP_SUBMIT_DAG, dagPayload);
            TitanProtocol.read(in);
        } catch (Exception e) { e.printStackTrace(); }
    }
}
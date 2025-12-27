package tests;

import network.RpcWorkerServer;
import network.TitanProtocol;
import scheduler.Scheduler;
import scheduler.Job;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class TitanDAGEndToEnd {
    private static Scheduler scheduler;

    public static void main(String[] args) throws Exception {
        scheduler = new Scheduler(9090);
        scheduler.start();

        RpcWorkerServer worker = new RpcWorkerServer(8080, "localhost", 9090, "TEST");
        new Thread(() -> { try { worker.start(); } catch (Exception e) {} }).start();
        Thread.sleep(2000);

        testSimpleSequence();
        testDiamondDag();
        testCascadingFailure();

        System.out.println("\n=== 🏁 ALL VALIDATIONS COMPLETE ===");
        System.exit(0);
    }

    private static void testSimpleSequence() throws Exception {
        System.out.println("\n--- 1. Validating Simple Sequence (A -> B) ---");
        sendDag("S1_A|TEST|DataA|2|0|[] ; S1_B|TEST|DataB|1|0|[S1_A]");

        Thread.sleep(4000); // Wait for processing

        assertStatus("S1_A", Job.Status.COMPLETED);
        assertStatus("S1_B", Job.Status.COMPLETED);
    }

    private static void testDiamondDag() throws Exception {
        System.out.println("\n--- 2. Validating Diamond DAG ---");

        sendDag("D_ROOT|TEST|Root|2|0|[] ; " +
                "D_LEFT|TEST|Left|1|0|[D_ROOT] ; " +
                "D_RIGHT|TEST|Right|1|0|[D_ROOT] ; " +
                "D_FINAL|TEST|Final|1|0|[D_LEFT,D_RIGHT]");

        Thread.sleep(6000);

        assertStatus("D_ROOT", Job.Status.COMPLETED);
        assertStatus("D_LEFT", Job.Status.COMPLETED);
        assertStatus("D_RIGHT", Job.Status.COMPLETED);
        assertStatus("D_FINAL", Job.Status.COMPLETED);
    }

    private static void testCascadingFailure() throws Exception {
        System.out.println("\n--- 3. Validating Cascading Failure ---");

        sendDag("F_PARENT|TEST|FAIL_THIS|2|0|[] ; " +
                "F_CHILD|TEST|Child|1|0|[F_PARENT] ; " +
                "F_GRAND|TEST|Grandchild|1|0|[F_CHILD]");

        // Wait longer because of retries
        Thread.sleep(12000);

        assertStatus("F_PARENT", Job.Status.DEAD); // Should be dead after 3 retries
        assertStatus("F_CHILD", Job.Status.DEAD);  // Should be cancelled/dead
        assertStatus("F_GRAND", Job.Status.DEAD);  // Should be cancelled/dead
    }

    private static void assertStatus(String id, Job.Status expected) throws InterruptedException {
        Job.Status actual = Job.Status.PENDING;
        // Poll the status for up to 5 seconds instead of checking once
        for (int i = 0; i < 10; i++) {
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
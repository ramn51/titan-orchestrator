package titan.scheduler;

import org.junit.jupiter.api.Test;
import titan.network.SchedulerServer;
import titan.network.TitanProtocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ServiceDeploymentReadinessTest {
    private static final class FakeWorker implements AutoCloseable {
        final ServerSocket listener;
        final CountDownLatch started = new CountDownLatch(1);
        final AtomicInteger stops = new AtomicInteger();
        final AtomicBoolean open = new AtomicBoolean(true);
        final Thread thread;

        FakeWorker() throws Exception {
            listener = new ServerSocket(0);
            listener.setSoTimeout(100);
            thread = new Thread(() -> {
                while (open.get()) {
                    try (Socket socket = listener.accept()) {
                        TitanProtocol.TitanPacket request = TitanProtocol.read(
                                new DataInputStream(socket.getInputStream()));
                        String reply = switch (request.opCode) {
                            case TitanProtocol.OP_STAGE -> "FILE_SAVED";
                            case TitanProtocol.OP_START_SERVICE -> "DEPLOYED_SUCCESS | PID: 1234";
                            case TitanProtocol.OP_STOP -> { stops.incrementAndGet(); yield "STOPPED"; }
                            default -> "DONE";
                        };
                        TitanProtocol.send(new DataOutputStream(socket.getOutputStream()),
                                TitanProtocol.OP_ACK, reply);
                        if (request.opCode == TitanProtocol.OP_START_SERVICE) started.countDown();
                    } catch (SocketTimeoutException ignored) {
                    } catch (Exception error) {
                        if (open.get()) throw new AssertionError(error);
                    }
                }
            });
            thread.setDaemon(true);
            thread.start();
        }

        int port() { return listener.getLocalPort(); }
        @Override public void close() throws Exception {
            open.set(false);
            listener.close();
            thread.join(1000);
        }
    }

    private static Thread dispatch(Scheduler scheduler) throws Exception {
        Method loop = Scheduler.class.getDeclaredMethod("runDispatchLoop");
        loop.setAccessible(true);
        Thread thread = new Thread(() -> {
            try { loop.invoke(scheduler); }
            catch (Exception error) {
                if (!(error.getCause() instanceof InterruptedException)) throw new AssertionError(error);
            }
        });
        thread.start();
        return thread;
    }

    private static void awaitStatus(Job job, Job.Status expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (job.getStatus() != expected && System.nanoTime() < deadline) Thread.sleep(5);
        assertEquals(expected, job.getStatus());
    }

    private static Job service(String id, int port) {
        return new Job(id, "GENERAL|DEPLOY_PAYLOAD|service.py|YQ==|" + port + "|GENERAL",
                1, 0, List.of());
    }

    private static TitanProtocol.TitanPacket resolveOverProtocol(Scheduler scheduler, String id) throws Exception {
        Field field = Scheduler.class.getDeclaredField("schedulerServer");
        field.setAccessible(true);
        SchedulerServer master = (SchedulerServer) field.get(scheduler);
        try (ServerSocket listener = new ServerSocket(0);
             Socket client = new Socket("127.0.0.1", listener.getLocalPort());
             Socket accepted = listener.accept()) {
            Thread handler = new Thread(() -> master.clientHandler(accepted));
            handler.start();
            TitanProtocol.send(new DataOutputStream(client.getOutputStream()),
                    TitanProtocol.OP_RESOLVE_SERVICE, id);
            TitanProtocol.TitanPacket response = TitanProtocol.read(new DataInputStream(client.getInputStream()));
            handler.join(1000);
            return response;
        }
    }

    @Test void dispatchContinuesWhileServiceWaitsAndDependentsUnlockOnlyAfterReady() throws Exception {
        try (FakeWorker worker = new FakeWorker();
             ServiceReadinessMonitor monitor = new ServiceReadinessMonitor(
                     ServiceDeploymentReadinessTest::reachable, 30, 25);
             ServerSocket portReservation = new ServerSocket(0)) {
            int servicePort = portReservation.getLocalPort();
            portReservation.close();
            Scheduler scheduler = new Scheduler(0, monitor);
            Thread dispatcher = null;
            try {
                scheduler.registerWorker("127.0.0.1", worker.port(), "GENERAL", true);
                dispatcher = dispatch(scheduler);
                Job deploy = service("DAG-service-ready", servicePort);
                Job child = new Job("DAG-service-child", "GENERAL|child", 1, 0,
                        List.of(deploy.getId()));
                scheduler.submitJob(deploy);
                scheduler.submitJob(child);
                assertTrue(worker.started.await(2, TimeUnit.SECONDS));
                assertEquals(Job.Status.RUNNING, deploy.getStatus());
                assertTrue(scheduler.resolveService(deploy.getId()).startsWith("ERROR"));
                assertTrue(scheduler.getDAGWaitingRoom().containsKey(child.getId()));

                assertThrows(IllegalArgumentException.class, () -> scheduler.submitJob(
                        service(deploy.getId(), servicePort)));
                Job duplicatePort = service("DAG-duplicate-port", servicePort);
                scheduler.submitJob(duplicatePort);
                awaitStatus(duplicatePort, Job.Status.FAILED);

                Job independent = new Job("DAG-independent", "GENERAL|independent", 1, 0, List.of());
                scheduler.submitJob(independent);
                awaitStatus(independent, Job.Status.COMPLETED);
                assertEquals(Job.Status.RUNNING, deploy.getStatus());

                try (ServerSocket listeningService = new ServerSocket(servicePort)) {
                    awaitStatus(deploy, Job.Status.COMPLETED);
                    assertEquals("127.0.0.1|" + servicePort, scheduler.resolveService(deploy.getId()));
                    TitanProtocol.TitanPacket resolved = resolveOverProtocol(scheduler, deploy.getId());
                    assertEquals(TitanProtocol.OP_ACK, resolved.opCode);
                    assertEquals("127.0.0.1|" + servicePort, resolved.payload);
                    awaitStatus(child, Job.Status.COMPLETED);
                }
            } finally {
                scheduler.stop();
                if (dispatcher != null) { dispatcher.interrupt(); dispatcher.join(2000); }
            }
        }
    }

    @Test void unreadyServiceFailsReleasesCapacityAndCancelsDependents() throws Exception {
        try (FakeWorker worker = new FakeWorker();
             ServiceReadinessMonitor monitor = new ServiceReadinessMonitor(
                     ServiceDeploymentReadinessTest::reachable, 3, 20);
             ServerSocket portReservation = new ServerSocket(0)) {
            int servicePort = portReservation.getLocalPort();
            portReservation.close();
            Scheduler scheduler = new Scheduler(0, monitor);
            Thread dispatcher = null;
            try {
                scheduler.registerWorker("127.0.0.1", worker.port(), "GENERAL", true);
                dispatcher = dispatch(scheduler);
                Job deploy = service("DAG-service-failed", servicePort);
                Job child = new Job("DAG-failed-child", "GENERAL|child", 1, 0,
                        List.of(deploy.getId()));
                scheduler.submitJob(deploy);
                scheduler.submitJob(child);
                awaitStatus(deploy, Job.Status.FAILED);
                awaitStatus(child, Job.Status.CANCELLED);
                assertTrue(scheduler.resolveService(deploy.getId()).startsWith("ERROR"));
                Worker host = scheduler.getWorkerRegistry().getWorkers().iterator().next();
                assertEquals(0, host.getCurrentLoad());
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (worker.stops.get() == 0 && System.nanoTime() < deadline) Thread.sleep(5);
                assertEquals(1, worker.stops.get());
            } finally {
                scheduler.stop();
                if (dispatcher != null) { dispatcher.interrupt(); dispatcher.join(2000); }
            }
        }
    }

    @Test void workerExitNotificationFailsPendingServiceImmediately() throws Exception {
        try (FakeWorker worker = new FakeWorker();
             ServiceReadinessMonitor monitor = new ServiceReadinessMonitor(
                     ServiceDeploymentReadinessTest::reachable, 30, 50);
             ServerSocket portReservation = new ServerSocket(0)) {
            int servicePort = portReservation.getLocalPort();
            portReservation.close();
            Scheduler scheduler = new Scheduler(0, monitor);
            Thread dispatcher = null;
            try {
                scheduler.registerWorker("127.0.0.1", worker.port(), "GENERAL", true);
                dispatcher = dispatch(scheduler);
                Job deploy = service("DAG-service-exited", servicePort);
                Job child = new Job("DAG-exited-child", "GENERAL|child", 1, 0,
                        List.of(deploy.getId()));
                scheduler.submitJob(deploy);
                scheduler.submitJob(child);
                assertTrue(worker.started.await(2, TimeUnit.SECONDS));
                scheduler.unregisterService(deploy.getId());
                awaitStatus(deploy, Job.Status.FAILED);
                awaitStatus(child, Job.Status.CANCELLED);
                assertEquals(0, scheduler.getWorkerRegistry().getWorkers().iterator().next().getCurrentLoad());
                assertTrue(scheduler.resolveService(deploy.getId()).startsWith("ERROR"));
            } finally {
                scheduler.stop();
                if (dispatcher != null) { dispatcher.interrupt(); dispatcher.join(2000); }
            }
        }
    }

    private static boolean reachable(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(host, port), 50);
            return true;
        } catch (Exception error) { return false; }
    }
}

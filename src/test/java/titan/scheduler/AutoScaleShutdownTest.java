package titan.scheduler;

import org.junit.jupiter.api.Test;
import titan.network.TitanProtocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class AutoScaleShutdownTest {
    @Test void autoscaledWorkerIsIntentionallyStoppedBeforeKill() throws Exception {
        try (ServerSocket parentSocket = new ServerSocket(0);
             ServerSocket childSocket = new ServerSocket(0)) {
            CountDownLatch parentStopped = new CountDownLatch(1);
            CountDownLatch childKilled = new CountDownLatch(1);
            AtomicReference<Throwable> serverError = new AtomicReference<>();
            Thread parent = new Thread(() -> {
                try (Socket socket = parentSocket.accept()) {
                    TitanProtocol.TitanPacket request = TitanProtocol.read(new DataInputStream(socket.getInputStream()));
                    assertEquals(TitanProtocol.OP_STOP, request.opCode);
                    assertEquals("WRK-" + childSocket.getLocalPort() + "-test", request.payload);
                    parentStopped.countDown();
                    TitanProtocol.send(new DataOutputStream(socket.getOutputStream()),
                            TitanProtocol.OP_ACK, "STOPPED");
                } catch (Throwable error) { serverError.set(error); }
            });
            Thread child = new Thread(() -> {
                try (Socket socket = childSocket.accept()) {
                    assertEquals(0, parentStopped.getCount(),
                            "service supervisor must be told this stop is intentional first");
                    TitanProtocol.TitanPacket request = TitanProtocol.read(new DataInputStream(socket.getInputStream()));
                    assertEquals(TitanProtocol.OP_KILL_WORKER, request.opCode);
                    childKilled.countDown();
                    TitanProtocol.send(new DataOutputStream(socket.getOutputStream()),
                            TitanProtocol.OP_KILL_WORKER, "SUCCESS");
                } catch (Throwable error) { serverError.set(error); }
            });
            parent.setDaemon(true);
            child.setDaemon(true);
            parent.start();
            child.start();

            Scheduler scheduler = new Scheduler(0, new ServiceReadinessMonitor());
            try {
                scheduler.registerWorker("127.0.0.1", parentSocket.getLocalPort(), "GENERAL", true);
                scheduler.registerWorker("127.0.0.1", childSocket.getLocalPort(), "GENERAL", false);
                Worker parentWorker = scheduler.getWorkerRegistry().getWorkers().stream()
                        .filter(worker -> worker.port() == parentSocket.getLocalPort()).findFirst().orElseThrow();
                String serviceId = "WRK-" + childSocket.getLocalPort() + "-test";
                scheduler.getLiveServiceMap().put(serviceId, parentWorker);

                String result = scheduler.shutdownWorkerNode("127.0.0.1", childSocket.getLocalPort());
                assertTrue(result.startsWith("SUCCESS"), result);
                assertTrue(parentStopped.await(2, TimeUnit.SECONDS));
                assertTrue(childKilled.await(2, TimeUnit.SECONDS));
                assertNull(serverError.get());
                assertFalse(scheduler.getLiveServiceMap().containsKey(serviceId));
            } finally {
                scheduler.stop();
            }
        }
    }
}

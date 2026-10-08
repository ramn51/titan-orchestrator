package titan.network;

import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class WorkerRegistrationTest {
    @Test void workerRegistersAgainAndStopClosesListener() throws Exception {
        try (ServerSocket master = new ServerSocket(0); ServerSocket reservation = new ServerSocket(0)) {
            int workerPort = reservation.getLocalPort();
            reservation.close();
            master.setSoTimeout(15_000);
            CountDownLatch twice = new CountDownLatch(2);
            AtomicReference<Throwable> serverError = new AtomicReference<>();
            Thread responder = new Thread(() -> {
                try {
                    for (int i = 0; i < 2; i++) {
                        try (Socket socket = master.accept()) {
                            TitanProtocol.TitanPacket request = TitanProtocol.read(
                                    new DataInputStream(socket.getInputStream()));
                            assertEquals(TitanProtocol.OP_REGISTER, request.opCode);
                            TitanProtocol.send(new DataOutputStream(socket.getOutputStream()),
                                    TitanProtocol.OP_ACK, "REGISTERED");
                            twice.countDown();
                        }
                    }
                } catch (Throwable error) { serverError.set(error); }
            }, "fake-master");
            responder.setDaemon(true);
            responder.start();
            RpcWorkerServer worker = new RpcWorkerServer(workerPort, "127.0.0.1",
                    master.getLocalPort(), "GENERAL", true);
            Thread listener = new Thread(() -> {
                try { worker.start(); }
                catch (Exception error) { serverError.set(error); }
            }, "test-worker");
            listener.start();
            try {
                assertTrue(twice.await(14, TimeUnit.SECONDS), "worker should re-register periodically");
            } finally {
                worker.stop();
                listener.join(2_000);
            }
            assertFalse(listener.isAlive(), "stop must close the listening socket");
            assertNull(serverError.get());
        }
    }
}

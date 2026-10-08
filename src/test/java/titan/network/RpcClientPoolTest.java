package titan.network;

import org.junit.jupiter.api.Test;
import titan.scheduler.WorkerRegistry;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RpcClientPoolTest {
    @Test void reusesConnectionAndEvictsWhenWorkerLeaves() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            server.setSoTimeout(1500);
            AtomicInteger accepts = new AtomicInteger();
            Thread peer = new Thread(() -> {
                try {
                    while (!server.isClosed()) {
                        try (Socket socket = server.accept()) {
                            accepts.incrementAndGet();
                            socket.setSoTimeout(1000);
                            while (true) {
                                try {
                                    TitanProtocol.read(new DataInputStream(socket.getInputStream()));
                                    TitanProtocol.send(new DataOutputStream(socket.getOutputStream()), TitanProtocol.OP_ACK, "PONG");
                                } catch (SocketTimeoutException timeout) { continue; }
                                catch (Exception closed) { break; }
                            }
                        } catch (SocketTimeoutException timeout) { continue; }
                    }
                } catch (Exception ignored) { }
            });
            peer.setDaemon(true);
            peer.start();
            RpcClient client = new RpcClient(new WorkerRegistry());
            try {
                assertEquals("PONG", client.sendRequest("127.0.0.1", server.getLocalPort(), TitanProtocol.OP_HEARTBEAT, ""));
                assertEquals("PONG", client.sendRequest("127.0.0.1", server.getLocalPort(), TitanProtocol.OP_HEARTBEAT, ""));
                assertEquals(1, accepts.get());
                client.evict("127.0.0.1", server.getLocalPort());
                assertEquals("PONG", client.sendRequest("127.0.0.1", server.getLocalPort(), TitanProtocol.OP_HEARTBEAT, ""));
                assertEquals(2, accepts.get());
            } finally { client.close(); server.close(); }
        }
    }

    @Test void staleReusedSocketRetriesOnceOnFreshConnection() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            AtomicInteger accepts = new AtomicInteger();
            Thread peer = new Thread(() -> {
                try {
                    for (int i = 0; i < 2; i++) {
                        try (Socket socket = server.accept()) {
                            accepts.incrementAndGet();
                            TitanProtocol.read(new DataInputStream(socket.getInputStream()));
                            TitanProtocol.send(new DataOutputStream(socket.getOutputStream()), TitanProtocol.OP_ACK, "PONG");
                            // Deliberately close the first socket after its response.
                        }
                    }
                } catch (Exception error) { throw new AssertionError(error); }
            });
            peer.setDaemon(true);
            peer.start();
            RpcClient client = new RpcClient(new WorkerRegistry());
            try {
                assertEquals("PONG", client.sendRequest("127.0.0.1", server.getLocalPort(), TitanProtocol.OP_HEARTBEAT, ""));
                assertEquals("PONG", client.sendRequest("127.0.0.1", server.getLocalPort(), TitanProtocol.OP_HEARTBEAT, ""));
                assertEquals(2, accepts.get());
            } finally { client.close(); }
        }
    }

    @Test void concurrentRequestsNeverOpenMoreThanFourSocketsPerDestination() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            server.setSoTimeout(500);
            AtomicInteger accepts = new AtomicInteger();
            ExecutorService handlers = Executors.newCachedThreadPool();
            Thread acceptor = new Thread(() -> {
                while (!server.isClosed()) {
                    try {
                        Socket socket = server.accept();
                        accepts.incrementAndGet();
                        handlers.submit(() -> {
                            try (socket) {
                                while (true) {
                                    TitanProtocol.read(new DataInputStream(socket.getInputStream()));
                                    Thread.sleep(50);
                                    TitanProtocol.send(new DataOutputStream(socket.getOutputStream()), TitanProtocol.OP_ACK, "OK");
                                }
                            } catch (Exception ignored) { }
                        });
                    } catch (SocketTimeoutException ignored) { }
                    catch (Exception ignored) { break; }
                }
            });
            acceptor.setDaemon(true);
            acceptor.start();
            RpcClient client = new RpcClient(new WorkerRegistry());
            ExecutorService callers = Executors.newFixedThreadPool(16);
            try {
                List<Future<String>> responses = new ArrayList<>();
                for (int i = 0; i < 16; i++) {
                    responses.add(callers.submit(() -> client.sendRequest("127.0.0.1", server.getLocalPort(),
                            TitanProtocol.OP_HEARTBEAT, "")));
                }
                for (Future<String> response : responses) assertEquals("OK", response.get(10, TimeUnit.SECONDS));
                assertTrue(accepts.get() <= 4, "per-destination pool must cap concurrent sockets");
            } finally {
                callers.shutdownNow();
                client.close();
                server.close();
                handlers.shutdownNow();
            }
        }
    }
}

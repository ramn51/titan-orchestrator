/*
 * Copyright 2026 Ram Narayanan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */

package titan.manual;

import titan.network.RpcClient;
import titan.network.TitanProtocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Guards {@link RpcClient}'s connection pool. Self-contained: it stands up its own echo servers, so
 * it needs no Master, no worker and no network.
 *
 * Three properties are asserted, each corresponding to a way the pool could be wrong:
 *
 *   1. REUSE       a socket is reused across requests rather than opened per request. Without this
 *                  the pool is doing nothing and the whole change is pointless.
 *   2. ISOLATION   concurrent callers never share a socket. The Master calls sendRequest from the
 *                  dispatch thread and the heartbeat executor at the same time; if they shared one
 *                  connection their frames would interleave and each would read the other's reply.
 *                  The echo server returns the exact payload it was sent, so any reply that does
 *                  not match its request is proof of interleaving.
 *   3. LIVENESS    a pooled socket that has gone stale must NOT be reported as a dead worker.
 *                  sendRequest returning null is what makes the Scheduler mark a node dead and
 *                  re-queue its in-flight jobs, so a stale socket mistaken for a death would re-run
 *                  healthy work. A genuinely absent peer must still return null.
 *
 * Run: java -cp target/classes:target/test-classes titan.manual.ConnectionPoolTest
 */
public class ConnectionPoolTest {

    private static int passed = 0, failed = 0;

    public static void main(String[] args) throws Exception {
        System.out.println("=== RpcClient connection pool ===");
        testReuse();
        testConcurrentIsolation(1, 400);
        testConcurrentIsolation(8, 300);
        testConcurrentIsolation(32, 150);
        testStaleSocketIsNotADeath();
        testAbsentPeerIsADeath();

        System.out.println("\n==================================================");
        System.out.println(" RESULT: " + passed + "/" + (passed + failed) + " passed");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    private static void check(String what, boolean ok, String detail) {
        System.out.println("  [" + (ok ? "PASS" : "FAIL") + "] " + what + (detail.isEmpty() ? "" : " - " + detail));
        if (ok) passed++; else failed++;
    }

    // ------------------------------------------------------------------
    /** One caller, many requests: the server must see a single connection, not one per request. */
    private static void testReuse() throws Exception {
        System.out.println("\n=== 1. a single caller reuses one socket across requests ===");
        EchoServer srv = new EchoServer();
        RpcClient c = new RpcClient(null);
        int n = 200;
        int ok = 0;
        for (int i = 0; i < n; i++) {
            String want = "reuse-" + i;
            if (want.equals(c.sendRequest("127.0.0.1", srv.port, TitanProtocol.OP_HEARTBEAT, want))) ok++;
        }
        check("every request got its own correct reply", ok == n, ok + "/" + n);
        check("the server accepted 1 connection, not " + n,
                srv.accepted.get() == 1, srv.accepted.get() + " accepted");
        srv.stop();
    }

    // ------------------------------------------------------------------
    /**
     * Many callers at once. Each request carries a payload unique to its thread and iteration, and
     * the echo server returns it verbatim, so a mismatch can only mean two threads shared a socket.
     */
    private static void testConcurrentIsolation(int threads, int each) throws Exception {
        System.out.println("\n=== 2. " + threads + " concurrent caller(s) x " + each + " requests ===");
        EchoServer srv = new EchoServer();
        RpcClient c = new RpcClient(null);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<int[]>> fs = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int id = t;
            fs.add(pool.submit(() -> {
                go.await();
                int good = 0, bad = 0, nul = 0;
                for (int i = 0; i < each; i++) {
                    String want = "T" + id + "-R" + i + "-" + (id * 7919 + i);
                    String got = c.sendRequest("127.0.0.1", srv.port, TitanProtocol.OP_HEARTBEAT, want);
                    if (got == null) nul++;
                    else if (want.equals(got)) good++;
                    else bad++;
                }
                return new int[]{good, bad, nul};
            }));
        }
        go.countDown();
        int good = 0, bad = 0, nul = 0;
        for (Future<int[]> f : fs) { int[] r = f.get(); good += r[0]; bad += r[1]; nul += r[2]; }
        pool.shutdown();
        int total = threads * each;

        check("no reply was delivered to the wrong caller", bad == 0, bad + " mismatched of " + total);
        check("no request was mistaken for a dead peer", nul == 0, nul + " nulls");
        check("every request completed correctly", good == total, good + "/" + total);
        check("sockets were reused, not opened per request",
                srv.accepted.get() <= threads, srv.accepted.get() + " connections for " + total + " requests");
        srv.stop();
    }

    // ------------------------------------------------------------------
    /**
     * The pool is warmed, then every connection the server holds is dropped, which is what a worker
     * restart looks like from the client side. The next request must still succeed, on a fresh
     * socket, rather than returning null.
     */
    private static void testStaleSocketIsNotADeath() throws Exception {
        System.out.println("\n=== 3. a stale pooled socket must not read as a dead worker ===");
        EchoServer srv = new EchoServer();
        RpcClient c = new RpcClient(null);
        for (int i = 0; i < 5; i++) c.sendRequest("127.0.0.1", srv.port, TitanProtocol.OP_HEARTBEAT, "warm");
        check("the pool was warmed", srv.accepted.get() == 1, srv.accepted.get() + " connection");

        srv.dropAllConnections();      // the peer went away underneath a pooled socket
        Thread.sleep(200);

        String got = c.sendRequest("127.0.0.1", srv.port, TitanProtocol.OP_HEARTBEAT, "after-restart");
        check("the request succeeded on a retry instead of returning null",
                "after-restart".equals(got), String.valueOf(got));
        check("the retry opened a new connection", srv.accepted.get() == 2, srv.accepted.get() + " total");
        srv.stop();
    }

    // ------------------------------------------------------------------
    /** The other half of the contract: a peer that is genuinely gone must still return null. */
    private static void testAbsentPeerIsADeath() throws Exception {
        System.out.println("\n=== 4. an absent peer must still return null ===");
        EchoServer srv = new EchoServer();
        RpcClient c = new RpcClient(null);
        int port = srv.port;
        check("the peer answers while it is up",
                "alive".equals(c.sendRequest("127.0.0.1", port, TitanProtocol.OP_HEARTBEAT, "alive")), "");

        srv.stop();                    // gone for good, pooled socket left behind
        Thread.sleep(300);

        String got = c.sendRequest("127.0.0.1", port, TitanProtocol.OP_HEARTBEAT, "anyone-there");
        check("null is returned, so death detection still fires", got == null, String.valueOf(got));
    }

    // ------------------------------------------------------------------
    /** Speaks TitanProtocol and echoes the payload back, looping per connection like a real worker. */
    private static final class EchoServer {
        final ServerSocket server;
        final int port;
        final AtomicInteger accepted = new AtomicInteger();
        private final List<Socket> live = new ArrayList<>();
        private volatile boolean running = true;

        EchoServer() throws Exception {
            server = new ServerSocket(0);
            port = server.getLocalPort();
            Thread t = new Thread(this::acceptLoop, "echo-" + port);
            t.setDaemon(true);
            t.start();
            Thread.sleep(150);
        }

        private void acceptLoop() {
            while (running) {
                try {
                    Socket s = server.accept();
                    accepted.incrementAndGet();
                    synchronized (live) { live.add(s); }
                    Thread h = new Thread(() -> serve(s), "echo-conn");
                    h.setDaemon(true);
                    h.start();
                } catch (Exception e) { return; }
            }
        }

        private void serve(Socket s) {
            try (s;
                 DataInputStream in = new DataInputStream(s.getInputStream());
                 DataOutputStream out = new DataOutputStream(s.getOutputStream())) {
                while (true) {
                    TitanProtocol.TitanPacket p = TitanProtocol.read(in);
                    TitanProtocol.send(out, TitanProtocol.OP_ACK, p.payload);
                }
            } catch (Exception ignored) { }
        }

        void dropAllConnections() {
            synchronized (live) {
                for (Socket s : live) { try { s.close(); } catch (Exception ignored) { } }
                live.clear();
            }
        }

        void stop() {
            running = false;
            dropAllConnections();
            try { server.close(); } catch (Exception ignored) { }
        }
    }
}

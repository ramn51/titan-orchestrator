package titan;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import titan.network.TitanProtocol;
import static org.junit.jupiter.api.Assertions.*;

class TitanCLITest {
    private static void invoke(TitanCLI cli, String command) throws Exception {
        Method handler = TitanCLI.class.getDeclaredMethod("handleCommand", String.class);
        handler.setAccessible(true);
        handler.invoke(cli, command);
    }

    @Test void commandsUseExistingProtocolAndHitlDecisionsUseStore() throws Exception {
        try (ServerSocket master = new ServerSocket(0)) {
            master.setSoTimeout(2_000);
            List<TitanProtocol.TitanPacket> received = new ArrayList<>();
            CompletableFuture<Void> service = CompletableFuture.runAsync(() -> {
                try {
                    for (int i = 0; i < 11; i++) {
                        try (Socket socket = master.accept()) {
                            TitanProtocol.TitanPacket request = TitanProtocol.read(
                                    new DataInputStream(socket.getInputStream()));
                            received.add(request);
                            String response = request.opCode == TitanProtocol.OP_KV_GET
                                    && request.payload.startsWith("titan:hitl:status:") ? "WAITING" : "OK";
                            TitanProtocol.send(new DataOutputStream(socket.getOutputStream()),
                                    TitanProtocol.OP_ACK, response);
                        }
                    }
                } catch (Exception error) {
                    throw new RuntimeException(error);
                }
            });
            TitanCLI cli = new TitanCLI("127.0.0.1", master.getLocalPort());
            for (String command : List.of("json", "status DAG-A", "logs DAG-A", "cancel DAG-A",
                    "kv get owner", "kv set owner Ada Lovelace", "approve DAG-hitl-gate-A",
                    "reject DAG-hitl-gate-B", "resolve service-A")) invoke(cli, command);
            service.get();
            byte[] opcodes = {TitanProtocol.OP_STATS_JSON, TitanProtocol.OP_GET_JOB_STATUS,
                    TitanProtocol.OP_GET_LOGS, TitanProtocol.OP_CANCEL_JOB, TitanProtocol.OP_KV_GET,
                    TitanProtocol.OP_KV_SET, TitanProtocol.OP_KV_GET, TitanProtocol.OP_KV_SET,
                    TitanProtocol.OP_KV_GET, TitanProtocol.OP_KV_SET, TitanProtocol.OP_RESOLVE_SERVICE};
            String[] payloads = {"", "DAG-A", "DAG-A", "DAG-A", "owner", "owner|Ada Lovelace",
                    "titan:hitl:status:DAG-hitl-gate-A", "titan:hitl:status:DAG-hitl-gate-A|APPROVED",
                    "titan:hitl:status:DAG-hitl-gate-B", "titan:hitl:status:DAG-hitl-gate-B|REJECTED",
                    "service-A"};
            for (int i = 0; i < opcodes.length; i++) {
                assertEquals(opcodes[i], received.get(i).opCode, "opcode " + i);
                assertEquals(payloads[i], received.get(i).payload, "payload " + i);
            }
        }
    }

    @Test void missingArgumentsNeverReachMaster() throws Exception {
        try (ServerSocket master = new ServerSocket(0)) {
            master.setSoTimeout(100);
            TitanCLI cli = new TitanCLI("127.0.0.1", master.getLocalPort());
            for (String command : List.of("status", "logs", "cancel", "approve", "reject",
                    "kv get", "kv set owner", "dag", "run", "upload", "stop", "resolve",
                    "deploy api.py", "deploy api.py 0", "shutdown")) {
                invoke(cli, command);
            }
            assertThrows(java.net.SocketTimeoutException.class, master::accept);
        }
    }

    @Test void documentedStoreAliasAndOptionalDagPrefixWork() throws Exception {
        try (ServerSocket master = new ServerSocket(0)) {
            master.setSoTimeout(2_000);
            List<TitanProtocol.TitanPacket> received = new ArrayList<>();
            CompletableFuture<Void> service = CompletableFuture.runAsync(() -> {
                try {
                    for (int i = 0; i < 5; i++) {
                        try (Socket socket = master.accept()) {
                            TitanProtocol.TitanPacket request = TitanProtocol.read(
                                    new DataInputStream(socket.getInputStream()));
                            received.add(request);
                            String response = i == 0 ? "NULL" : i == 1 ? "RUNNING"
                                    : i == 3 ? "WAITING" : "OK";
                            TitanProtocol.send(new DataOutputStream(socket.getOutputStream()),
                                    TitanProtocol.OP_ACK, response);
                        }
                    }
                } catch (Exception error) { throw new RuntimeException(error); }
            });
            TitanCLI cli = new TitanCLI("127.0.0.1", master.getLocalPort());
            invoke(cli, "status taskA");
            invoke(cli, "store get owner");
            invoke(cli, "approve hitl-gate-A");
            service.get();
            assertEquals(List.of("taskA", "DAG-taskA", "owner",
                    "titan:hitl:status:DAG-hitl-gate-A",
                    "titan:hitl:status:DAG-hitl-gate-A|APPROVED"),
                    received.stream().map(packet -> packet.payload).toList());
        }
    }
}

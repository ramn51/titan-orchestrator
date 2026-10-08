package titan.network;

import org.junit.jupiter.api.Test;
import titan.scheduler.Scheduler;
import java.io.*;
import java.lang.reflect.Field;
import java.net.*;
import java.time.LocalDate;
import java.time.ZoneOffset;
import static org.junit.jupiter.api.Assertions.*;

class ObservabilityRpcTest {
    private static TitanProtocol.TitanPacket request(SchedulerServer server, byte opcode, String payload) throws Exception {
        try (ServerSocket listener = new ServerSocket(0);
             Socket client = new Socket("127.0.0.1", listener.getLocalPort());
             Socket accepted = listener.accept()) {
            Thread handler = new Thread(() -> server.clientHandler(accepted));
            handler.start();
            client.setSoTimeout(2000);
            TitanProtocol.send(new DataOutputStream(client.getOutputStream()), opcode, payload);
            TitanProtocol.TitanPacket response = TitanProtocol.read(new DataInputStream(client.getInputStream()));
            handler.join(2000);
            return response;
        }
    }

    @Test void clusterReadsReturnBoundedJsonAndInvalidHistoryDateIsError() throws Exception {
        Scheduler scheduler = new Scheduler(0);
        try {
            Field field = Scheduler.class.getDeclaredField("schedulerServer");
            field.setAccessible(true);
            SchedulerServer server = (SchedulerServer) field.get(scheduler);
            assertEquals("[]", request(server, TitanProtocol.OP_STATS_JSON, "timeline|0|20").payload);
            assertTrue(request(server, TitanProtocol.OP_STATS_JSON, "status|unknown").payload
                    .contains("\"status\":\"UNKNOWN\""));
            assertTrue(request(server, TitanProtocol.OP_STATS_JSON, "queue|20").payload
                    .contains("\"jobs\":[]"));
            assertTrue(request(server, TitanProtocol.OP_STATS_JSON, "metrics|0|20").payload
                    .contains("\"samples\":[]"));
            String history = request(server, TitanProtocol.OP_STATS_JSON,
                    "history|" + LocalDate.now(ZoneOffset.UTC) + "|20").payload;
            assertTrue(history.startsWith("[") && history.endsWith("]"));
            assertEquals(TitanProtocol.OP_ERROR,
                    request(server, TitanProtocol.OP_STATS_JSON, "history|invalid|20").opCode);
        } finally { scheduler.stop(); }
    }

    @Test void emptyStatsPayloadKeepsOriginalOrchestratorContract() throws Exception {
        Scheduler scheduler = new Scheduler(0);
        try {
            Field field = Scheduler.class.getDeclaredField("schedulerServer");
            field.setAccessible(true);
            SchedulerServer server = (SchedulerServer) field.get(scheduler);
            TitanProtocol.TitanPacket empty = request(server, TitanProtocol.OP_STATS_JSON, "");
            TitanProtocol.TitanPacket named = request(server, TitanProtocol.OP_STATS_JSON, "system");
            assertEquals(TitanProtocol.OP_ACK, empty.opCode);
            assertEquals(TitanProtocol.OP_ACK, named.opCode);
            assertTrue(empty.payload.startsWith("{"));
            assertTrue(empty.payload.contains("\"workers\""));
            assertTrue(named.payload.startsWith("{"));
            assertTrue(named.payload.contains("\"workers\""));
        } finally { scheduler.stop(); }
    }
}

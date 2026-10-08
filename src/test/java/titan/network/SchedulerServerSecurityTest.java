package titan.network;

import org.junit.jupiter.api.Test;
import titan.scheduler.Scheduler;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class SchedulerServerSecurityTest {
    private static SchedulerServer server(Scheduler scheduler) throws Exception {
        Field field = Scheduler.class.getDeclaredField("schedulerServer");
        field.setAccessible(true);
        return (SchedulerServer) field.get(scheduler);
    }

    private static TitanProtocol.TitanPacket request(SchedulerServer server, byte op, String payload) throws Exception {
        try (ServerSocket listener = new ServerSocket(0);
             Socket client = new Socket("127.0.0.1", listener.getLocalPort());
             Socket accepted = listener.accept()) {
            Thread handler = new Thread(() -> server.clientHandler(accepted));
            handler.start();
            client.setSoTimeout(2000);
            TitanProtocol.send(new DataOutputStream(client.getOutputStream()), op, payload);
            TitanProtocol.TitanPacket reply = TitanProtocol.read(new DataInputStream(client.getInputStream()));
            handler.join(2000);
            return reply;
        }
    }

    @Test void traversalAndCommandFailuresUseErrorOpcode() throws Exception {
        Scheduler scheduler = new Scheduler(0);
        try {
            SchedulerServer server = server(scheduler);
            String encoded = Base64.getEncoder().encodeToString("data".getBytes());
            assertEquals(TitanProtocol.OP_ERROR, request(server, TitanProtocol.OP_UPLOAD_ASSET,
                    "../outside.txt|" + encoded).opCode);
            assertEquals(TitanProtocol.OP_ERROR, request(server, TitanProtocol.OP_UPLOAD_ASSET,
                    "..\\outside.txt|" + encoded).opCode);
            assertEquals(TitanProtocol.OP_ERROR, request(server, TitanProtocol.OP_UPLOAD_ASSET,
                    "C:\\outside.txt|" + encoded).opCode);
            assertEquals(TitanProtocol.OP_ERROR, request(server, TitanProtocol.OP_DEPLOY_SCRIPT,
                    "../outside.py|" + encoded).opCode);
            assertEquals(TitanProtocol.OP_ERROR, request(server, TitanProtocol.OP_FETCH_ASSET,
                    "../outside.txt").opCode);
            assertEquals(TitanProtocol.OP_ERROR, request(server, TitanProtocol.OP_FETCH_ASSET,
                    "C:\\outside.txt").opCode);
            assertEquals(TitanProtocol.OP_ERROR, request(server, TitanProtocol.OP_DEPLOY,
                    "../outside.py|8085").opCode);
            assertEquals(TitanProtocol.OP_ERROR, request(server, TitanProtocol.OP_RUN,
                    "../outside.py").opCode);
            assertEquals(TitanProtocol.OP_ERROR, request(server, TitanProtocol.OP_GET_LOGS,
                    "../outside").opCode);
            assertEquals(TitanProtocol.OP_ERROR, request(server, TitanProtocol.OP_LOG_STREAM,
                    "../outside|malicious log").opCode);
            assertEquals(TitanProtocol.OP_ERROR, request(server, TitanProtocol.OP_LOG_BATCH,
                    "../outside|malicious log").opCode);
            assertEquals(TitanProtocol.OP_ERROR, request(server, TitanProtocol.OP_SUBMIT_JOB,
                    "../outside|GENERAL|work|1|0").opCode);
            assertEquals(TitanProtocol.OP_ERROR, request(server, TitanProtocol.OP_SUBMIT_DAG,
                    "../outside|GENERAL|work|1|0|[]").opCode);
            assertEquals(TitanProtocol.OP_ERROR, request(server, (byte) 0x7f, "").opCode);
            assertEquals(TitanProtocol.OP_ERROR, request(server, TitanProtocol.OP_RESOLVE_SERVICE,
                    "missing").opCode);
        } finally {
            scheduler.stop();
        }
    }

    @Test void validFilesSucceedAndLogTextIsNotMistakenForCommandFailure() throws Exception {
        Scheduler scheduler = new Scheduler(0);
        String name = "security-" + UUID.randomUUID() + ".txt";
        String logId = "security-" + UUID.randomUUID();
        Path uploaded = Path.of("uploads", name);
        Path log = Path.of("titan_server_logs", logId + ".log");
        try {
            SchedulerServer server = server(scheduler);
            String encoded = Base64.getEncoder().encodeToString("data".getBytes());
            assertEquals(TitanProtocol.OP_ACK, request(server, TitanProtocol.OP_UPLOAD_ASSET,
                    name + "|" + encoded).opCode);
            TitanProtocol.TitanPacket fetched = request(server, TitanProtocol.OP_FETCH_ASSET, name);
            assertEquals(TitanProtocol.OP_ACK, fetched.opCode);
            assertEquals(encoded, fetched.payload);
            assertEquals(TitanProtocol.OP_ACK, request(server, TitanProtocol.OP_LOG_STREAM,
                    logId + "|ERROR: this is a log line").opCode);
            TitanProtocol.TitanPacket logs = request(server, TitanProtocol.OP_GET_LOGS, logId);
            assertEquals(TitanProtocol.OP_ACK, logs.opCode);
            assertTrue(logs.payload.contains("ERROR: this is a log line"), logs.payload);
        } finally {
            Files.deleteIfExists(uploaded);
            Files.deleteIfExists(log);
            scheduler.stop();
        }
    }
}

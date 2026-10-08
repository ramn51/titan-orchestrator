package titan.network;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CompletionCallbackSenderTest {
    @Test void retriesUnacknowledgedDeliveryAndStopsAfterAcknowledgment() throws Exception {
        try (ServerSocket master = new ServerSocket(0)) {
            master.setSoTimeout(1_000);
            CompletableFuture<Integer> received = CompletableFuture.supplyAsync(() -> {
                try {
                    for (int attempt = 1; attempt <= 2; attempt++) {
                        try (Socket socket = master.accept()) {
                            TitanProtocol.TitanPacket packet = TitanProtocol.read(
                                    new DataInputStream(socket.getInputStream()));
                            assertEquals(TitanProtocol.OP_JOB_COMPLETE, packet.opCode);
                            assertEquals("DAG-A|FAILED|exit 1", packet.payload);
                            TitanProtocol.send(new DataOutputStream(socket.getOutputStream()),
                                    attempt == 1 ? TitanProtocol.OP_ERROR : TitanProtocol.OP_ACK,
                                    attempt == 1 ? "TEMPORARY_ERROR" : "ACK_CALLBACK");
                        }
                    }
                    master.setSoTimeout(200);
                    try (Socket unexpected = master.accept()) {
                        fail("Callback sent again after acknowledgment");
                    } catch (SocketTimeoutException expected) {
                        return 2;
                    }
                    return -1;
                } catch (Exception error) {
                    throw new RuntimeException(error);
                }
            });
            assertTrue(CompletionCallbackSender.send("127.0.0.1", master.getLocalPort(),
                    "DAG-A", "FAILED", "exit 1", 3, 10));
            assertEquals(2, received.get());
        }
    }

    @Test void stopsAfterBoundedConnectionFailures() throws Exception {
        int unusedPort;
        try (ServerSocket reservation = new ServerSocket(0)) {
            unusedPort = reservation.getLocalPort();
        }
        assertFalse(CompletionCallbackSender.send("127.0.0.1", unusedPort,
                "DAG-B", "COMPLETED", "ok", 3, 5));
    }
}

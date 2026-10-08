package titan.storage;

import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TitanJRedisAdapterReconnectTest {
    @Test void nextCommandReconnectsAfterConnectionDrops() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            CompletableFuture<Void> peer = CompletableFuture.runAsync(() -> {
                try {
                    try (Socket first = server.accept()) {
                        first.getInputStream().read();
                    }
                    try (Socket second = server.accept()) {
                        second.getInputStream().read();
                        second.getOutputStream().write("$2\r\nok\r\n".getBytes(StandardCharsets.UTF_8));
                        second.getOutputStream().flush();
                    }
                } catch (Exception error) {
                    throw new RuntimeException(error);
                }
            });
            try (TitanJRedisAdapter adapter = new TitanJRedisAdapter("127.0.0.1", server.getLocalPort())) {
                assertNull(adapter.get("key"));
                assertEquals("ok", adapter.get("key"));
                peer.get(5, TimeUnit.SECONDS);
            }
        }
    }
}

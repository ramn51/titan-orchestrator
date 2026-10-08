package titan.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/** Delivers one worker result until the master acknowledges it or the retry budget expires. */
final class CompletionCallbackSender {
    private static final int ATTEMPTS = 5;
    private static final long FIRST_DELAY_MS = 250;
    private static final int SOCKET_TIMEOUT_MS = 2_000;

    private CompletionCallbackSender() {}

    static boolean send(String host, int port, String jobId, String status, String result) {
        return send(host, port, jobId, status, result, ATTEMPTS, FIRST_DELAY_MS);
    }

    static boolean send(String host, int port, String jobId, String status, String result, int workerPort) {
        return deliver(host, port, jobId, status,
                jobId + "|" + status + "|worker=" + workerPort + "|" + result,
                ATTEMPTS, FIRST_DELAY_MS);
    }

    // Package-private policy parameters keep the retry contract quick to verify in tests.
    static boolean send(String host, int port, String jobId, String status, String result,
                        int attempts, long firstDelayMs) {
        return deliver(host, port, jobId, status, jobId + "|" + status + "|" + result,
                attempts, firstDelayMs);
    }

    private static boolean deliver(String host, int port, String jobId, String status, String payload,
                                   int attempts, long firstDelayMs) {
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), SOCKET_TIMEOUT_MS);
                socket.setSoTimeout(SOCKET_TIMEOUT_MS);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                DataInputStream in = new DataInputStream(socket.getInputStream());
                TitanProtocol.send(out, TitanProtocol.OP_JOB_COMPLETE, payload);
                TitanProtocol.TitanPacket acknowledgement = TitanProtocol.read(in);
                if (acknowledgement.opCode != TitanProtocol.OP_ACK
                        || !"ACK_CALLBACK".equals(acknowledgement.payload)) {
                    throw new IOException("Unexpected callback response: " + acknowledgement.payload);
                }
                return true;
            } catch (Exception error) {
                if (attempt == attempts) {
                    System.err.println("[FAIL] Callback for " + jobId + " was not acknowledged after "
                            + attempts + " attempts: " + error.getMessage());
                    return false;
                }
                long delay = firstDelayMs * (1L << (attempt - 1));
                System.err.println("[WARN] Callback for " + jobId + " attempt " + attempt
                        + " failed: " + error.getMessage() + "; retrying in " + delay + " ms");
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    System.err.println("[FAIL] Callback for " + jobId + " interrupted before acknowledgment");
                    return false;
                }
            }
        }
        return false;
    }
}

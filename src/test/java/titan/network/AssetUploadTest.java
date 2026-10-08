package titan.network;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import titan.scheduler.Scheduler;
import titan.filesys.AssetManager;
import static org.junit.jupiter.api.Assertions.*;

class AssetUploadTest {
    private static String command(SchedulerServer server, byte opcode, String payload) throws Exception {
        Method method = SchedulerServer.class.getDeclaredMethod("processCommand", TitanProtocol.TitanPacket.class);
        method.setAccessible(true);
        return (String) method.invoke(server, new TitanProtocol.TitanPacket(opcode, payload));
    }

    @Test void uploadStaysInUploadsAndCanBeFetched() throws Exception {
        Scheduler scheduler = new Scheduler(0);
        String name = "titan-asset-test-" + UUID.randomUUID() + ".txt";
        Path uploaded = Path.of("uploads", name);
        try {
            java.lang.reflect.Field field = Scheduler.class.getDeclaredField("schedulerServer");
            field.setAccessible(true);
            SchedulerServer server = (SchedulerServer) field.get(scheduler);
            String encoded = Base64.getEncoder().encodeToString("artifact".getBytes());
            assertEquals("UPLOAD_SUCCESS", command(server, TitanProtocol.OP_UPLOAD_ASSET, name + "|" + encoded));
            assertArrayEquals("artifact".getBytes(), Files.readAllBytes(uploaded));
            assertEquals(encoded, command(server, TitanProtocol.OP_FETCH_ASSET, name));
            assertTrue(command(server, TitanProtocol.OP_UPLOAD_ASSET, "../escape.txt|" + encoded)
                    .startsWith("ERROR:"));
        } finally {
            Files.deleteIfExists(uploaded);
            scheduler.stop();
        }
    }

    @Test void deployScriptValidatesNameAndPreservesExistingFile() throws Exception {
        Scheduler scheduler = new Scheduler(0);
        String name = "titan-stage-test-" + UUID.randomUUID() + ".py";
        Path staged = Path.of("perm_files", name);
        try {
            java.lang.reflect.Field field = Scheduler.class.getDeclaredField("schedulerServer");
            field.setAccessible(true);
            SchedulerServer server = (SchedulerServer) field.get(scheduler);
            String encoded = Base64.getEncoder().encodeToString("print('ok')\n".getBytes());
            assertEquals("SCRIPT_STAGED:" + name,
                    command(server, TitanProtocol.OP_DEPLOY_SCRIPT, name + "|" + encoded));
            assertArrayEquals("print('ok')\n".getBytes(), Files.readAllBytes(staged));
            assertEquals("ERROR: Script already exists",
                    command(server, TitanProtocol.OP_DEPLOY_SCRIPT, name + "|" + encoded));
            assertEquals("ERROR: Invalid script filename",
                    command(server, TitanProtocol.OP_DEPLOY_SCRIPT, "../escape.py|" + encoded));
            assertEquals("ERROR: Invalid script filename",
                    command(server, TitanProtocol.OP_DEPLOY_SCRIPT, "..\\escape.py|" + encoded));
            assertEquals("ERROR: Invalid Base64 script content",
                    command(server, TitanProtocol.OP_DEPLOY_SCRIPT, "new-stage-test.py|%%%"));
        } finally {
            Files.deleteIfExists(staged);
            scheduler.stop();
        }
    }

    @Test void uploadedArchivePointerReadsFromUploads() throws Exception {
        String name = "titan-archive-test-" + UUID.randomUUID() + ".zip";
        Path uploaded = Path.of("uploads", name);
        Files.createDirectories(uploaded.getParent());
        try {
            Files.write(uploaded, "archive".getBytes());
            AssetManager.ArchiveInfo resolved = AssetManager.resolvePointer(name + "/main.py");
            assertEquals(name, resolved.zipName);
            assertEquals("main.py", resolved.entryPoint);
            assertEquals(Base64.getEncoder().encodeToString("archive".getBytes()), resolved.base64Content);
            assertThrows(java.io.IOException.class,
                    () -> AssetManager.resolvePointer("../" + name + "/main.py"));
        } finally {
            Files.deleteIfExists(uploaded);
        }
    }
}

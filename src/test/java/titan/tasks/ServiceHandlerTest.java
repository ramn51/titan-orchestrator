package titan.tasks;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ServiceHandlerTest {
    @Test void childWorkerReceivesTheParentsRemoteMasterEndpoint() {
        File workerJar = new File("titan_workspace", "Worker.jar").getAbsoluteFile();

        List<String> command = ServiceHandler.workerCommand(
                "/opt/java/bin/java", workerJar, "8090", "10.168.0.2", 9090);

        assertEquals(List.of(
                "/opt/java/bin/java", "-jar", workerJar.getAbsolutePath(), "8090",
                "10.168.0.2", "9090", "GENERAL", "false"), command);
    }

    @Test void nestedAutoscaledWorkerUsesTheClusterConfigurationDirectory(@TempDir Path temp) throws Exception {
        Path cluster = Files.createDirectories(temp.resolve("cluster"));
        Files.writeString(cluster.resolve("config.properties"), "titan.worker.slots=1");
        Path nestedWorkspace = Files.createDirectories(
                cluster.resolve("titan_workspace").resolve("titan_workspace"));
        File workerJar = Files.createFile(nestedWorkspace.resolve("Worker.jar")).toFile();

        assertEquals(cluster.toFile(), ServiceHandler.workerLaunchDirectory(workerJar));
    }

    @Test void workerWithoutAClusterConfigRunsBesideItsJar(@TempDir Path temp) throws Exception {
        File workerJar = Files.createFile(temp.resolve("Worker.jar")).toFile();

        assertEquals(temp.toFile(), ServiceHandler.workerLaunchDirectory(workerJar));
    }

    @Test void childWorkerCannotSilentlyFallBackToLocalhost() {
        File workerJar = new File("titan_workspace", "Worker.jar").getAbsoluteFile();

        assertThrows(IllegalArgumentException.class,
                () -> ServiceHandler.workerCommand("java", workerJar, "8090", " ", 9090));
    }
}

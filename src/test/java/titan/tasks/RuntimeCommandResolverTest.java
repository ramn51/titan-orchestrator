package titan.tasks;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RuntimeCommandResolverTest {
    private static List<String> resolve(String filename, String os, Map<String, String> settings) {
        return RuntimeCommandResolver.commandFor(new File(filename), os,
                key -> settings.getOrDefault(key, ""));
    }

    @Test void pythonDefaultsArePlatformAwareAndConfigurable() {
        assertEquals("python", resolve("task.py", "Windows 11", Map.of()).get(0));
        assertEquals("python3", resolve("task.py", "Linux", Map.of()).get(0));
        assertEquals("/opt/python", resolve("task.py", "Linux",
                Map.of("titan.python.executable", "/opt/python")).get(0));
    }

    @Test void windowsUsesNativeBatchAndPowerShellLaunchers() {
        List<String> batch = resolve("task.cmd", "Windows 11", Map.of());
        assertEquals(List.of("cmd.exe", "/d", "/s", "/c"), batch.subList(0, 4));
        List<String> powershell = resolve("task.ps1", "Windows 11", Map.of());
        assertEquals("powershell.exe", powershell.get(0));
        assertTrue(powershell.contains("-File"));
    }

    @Test void unixRejectsWindowsBatchWithAUsefulError() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> resolve("task.bat", "Linux", Map.of()));
        assertTrue(error.getMessage().contains("Windows worker"));
    }

    @Test void supportedPayloadExtensionsAreCaseInsensitive() {
        assertTrue(RuntimeCommandResolver.isSupportedScript("TASK.PY"));
        assertTrue(RuntimeCommandResolver.isSupportedScript("deploy.PS1"));
        assertTrue(RuntimeCommandResolver.isSupportedScript("run.cmd"));
    }
}

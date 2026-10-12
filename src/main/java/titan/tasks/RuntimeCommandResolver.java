/*
 * Copyright 2026 Ram Narayanan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package titan.tasks;

import titan.TitanConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

/** Builds process commands without embedding one operating system's paths. */
final class RuntimeCommandResolver {
    private RuntimeCommandResolver() { }

    static boolean isSupportedScript(String filename) {
        String lower = filename == null ? "" : filename.toLowerCase(Locale.ROOT);
        return lower.endsWith(".py") || lower.endsWith(".sh")
                || lower.endsWith(".bat") || lower.endsWith(".cmd")
                || lower.endsWith(".ps1");
    }

    static List<String> commandFor(File script) {
        return commandFor(script, System.getProperty("os.name"),
                key -> TitanConfig.get(key, ""));
    }

    static List<String> commandFor(File script, String osName,
                                   Function<String, String> setting) {
        String path = script.getAbsolutePath();
        String lower = script.getName().toLowerCase(Locale.ROOT);
        boolean windows = isWindows(osName);
        List<String> command = new ArrayList<>();

        if (lower.endsWith(".py")) {
            command.add(value(setting, "titan.python.executable", windows ? "python" : "python3"));
            command.add("-u");
            command.add(path);
        } else if (lower.endsWith(".sh")) {
            String fallback = windows ? "bash"
                    : (new File("/bin/bash").isFile() ? "/bin/bash" : "/bin/sh");
            command.add(value(setting, "titan.shell.executable", fallback));
            command.add(path);
        } else if (lower.endsWith(".bat") || lower.endsWith(".cmd")) {
            if (!windows) {
                throw new IllegalArgumentException("Windows batch scripts require a Windows worker: "
                        + script.getName());
            }
            command.add(value(setting, "titan.cmd.executable", "cmd.exe"));
            command.add("/d");
            command.add("/s");
            command.add("/c");
            command.add(path);
        } else if (lower.endsWith(".ps1")) {
            command.add(value(setting, "titan.powershell.executable",
                    windows ? "powershell.exe" : "pwsh"));
            command.add("-NoProfile");
            command.add("-NonInteractive");
            command.add("-File");
            command.add(path);
        } else {
            command.add(path);
        }
        return command;
    }

    private static String value(Function<String, String> setting, String key, String fallback) {
        String configured = setting.apply(key);
        return configured == null || configured.isBlank() ? fallback : configured.trim();
    }

    private static boolean isWindows(String osName) {
        return osName != null && osName.toLowerCase(Locale.ROOT).contains("win");
    }
}

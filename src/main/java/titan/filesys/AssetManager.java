/*
 * Copyright 2026 Ram Narayanan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */

package titan.filesys;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;

public class AssetManager {

    private static final String STORAGE_DIR = "perm_files";

    public static class ArchiveInfo {
        public final String zipName;
        public final String entryPoint;
        public final String base64Content;

        public ArchiveInfo(String zipName, String entryPoint, String base64Content) {
            this.zipName = zipName;
            this.entryPoint = entryPoint;
            this.base64Content = base64Content;
        }
    }

    /**
     * Resolves a pointer string (e.g., "my_project.zip/src/main.py")
     * into the raw Base64 data needed for the worker.
     */
    public static ArchiveInfo resolvePointer(String pointer) throws IOException {
        // 1. Parse the string
        int zipIndex = pointer.indexOf(".zip");
        if (zipIndex == -1) {
            throw new IOException("Invalid Archive Pointer: Must contain .zip (e.g. proj.zip/main.py)");
        }

        String zipName = pointer.substring(0, zipIndex + 4); // "my_project.zip"

        // Handle case where pointer is just "proj.zip" vs "proj.zip/main.py"
        String entryPoint = "";
        if (pointer.length() > zipIndex + 5) {
            entryPoint = pointer.substring(zipIndex + 5); // "src/main.py"
        }

        File zipFile = new File(STORAGE_DIR, zipName);
        if (!zipFile.exists()) {
            throw new IOException("Asset not found: " + zipName + " (Did you upload it to " + STORAGE_DIR + "?)");
        }


        byte[] fileBytes = Files.readAllBytes(zipFile.toPath());
        String base64 = Base64.getEncoder().encodeToString(fileBytes);
        return new ArchiveInfo(zipName, entryPoint, base64);
    }
}
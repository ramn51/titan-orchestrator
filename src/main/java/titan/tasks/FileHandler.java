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

package titan.tasks;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;

/**
 * Handles file-related tasks, specifically saving files to a designated workspace directory.
 * This class implements the {@link TaskHandler} interface, providing an execution mechanism
 * for file operations. It decodes a Base64-encoded file content from a payload and writes
 * it to a file within the {@value #WORKSPACE_DIR} directory.
 */
    public class FileHandler implements TaskHandler {
    /**
 * The base directory where all files processed by this handler will be saved.
 * This directory is created if it does not already exist upon instantiation of {@code FileHandler}.
 */
    private static final String WORKSPACE_DIR = "./titan_workspace";

    /**
 * Constructs a new {@code FileHandler} instance.
 * This constructor ensures that the {@value #WORKSPACE_DIR} directory exists.
 * If the directory does not exist, it attempts to create it, along with any necessary but nonexistent parent directories.
 */
    public FileHandler(){
        new File(WORKSPACE_DIR).mkdirs();
    }

    @Override
    /**
 * Executes a file saving operation based on the provided payload.
 * The payload is expected to be a string in the format "fileName|base64EncodedData".
 * The method decodes the Base64 data and writes it to a file named {@code fileName}
 * within the {@value #WORKSPACE_DIR} directory.
 *
 * @param payload A string containing the file name and its Base64-encoded content,
 *                separated by a pipe character (e.g., "myFile.txt|SGVsbG8gV29ybGQh").
 * @return A string indicating the successful saving of the file, including its absolute path.
 *         Format: "FILE_SAVED: /absolute/path/to/file".
 * @throws IllegalArgumentException If the payload format is invalid (e.g., missing the pipe separator or data).
 * @throws RuntimeException If an {@link IOException} occurs during the file writing process.
 */
    public String execute(String payload) {
        String[] parts = payload.split("\\|", 2);
        if(parts.length < 2) throw new IllegalArgumentException("Invalid File Payload");

        String fileName = parts[0];
        byte[] data = Base64.getDecoder().decode(parts[1]);

        Path destination = Paths.get(WORKSPACE_DIR, fileName);
        try {
            Files.write(destination, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return "FILE_SAVED: " + destination.toAbsolutePath();
    }
}

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

public class FileHandler implements TaskHandler {
    private static final String WORKSPACE_DIR = "./titan_workspace";

    public FileHandler(){
        new File(WORKSPACE_DIR).mkdirs();
    }

    @Override
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

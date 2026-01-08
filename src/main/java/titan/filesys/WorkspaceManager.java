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
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

public class WorkspaceManager {
    private static final String ROOT_DIR = "titan_workspace";

    public static File setupWorkspace(String id) throws IOException {
        File workspace = new File(ROOT_DIR, id);

        if(workspace.exists()){
            deleteRecursively(workspace.toPath());
        }

        if(!workspace.mkdirs()){
            throw new IOException("Failed to create workspace: " + workspace.getAbsolutePath());
        }

        return workspace;
    }

    public static File stageArchive(String id, String base64Zip) throws IOException{
        File workspace = setupWorkspace(id);
        ZipUtils.unzipBase64(base64Zip, workspace);
        System.out.println("[INFO] [WORKSPACE_MANAGER] Staged archive for " + id + " at " + workspace.getAbsolutePath());
        return workspace;
    }

    public static String resolvePath(String id, String filename){
        File workspace = new File(ROOT_DIR, id);
        File target = new File(workspace, filename);
        return target.getAbsolutePath();
    }

    private static void deleteRecursively(Path path) throws IOException{
        if(!Files.exists(path)) return;
        try(Stream<Path> walk = Files.walk(path)){
            walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }
}

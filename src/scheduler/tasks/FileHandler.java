package scheduler.tasks;

import scheduler.TaskHandler;

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

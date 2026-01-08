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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipUtils {
    public static void unzipBase64(String base64Data, File targetDir) throws IOException{
        byte[] zipBytes = Base64.getDecoder().decode(base64Data);
        try(ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes))){
            ZipEntry entry;
            while((entry = zis.getNextEntry()) !=null){
                File newFile = new File(targetDir, entry.getName());
                // Prevent "Zip Slip" (writing outside targetDir)
                if (!newFile.getCanonicalPath().startsWith(targetDir.getCanonicalPath())) {
                    throw new IOException("Security Error: Zip entry outside target directory: " + entry.getName());
                }

                if (entry.isDirectory()) {
                    newFile.mkdirs();
                } else {
                    new File(newFile.getParent()).mkdirs();
                    try (FileOutputStream fos = new FileOutputStream(newFile)) {
                        zis.transferTo(fos);
                    }
                }
            }
        }
    }
}

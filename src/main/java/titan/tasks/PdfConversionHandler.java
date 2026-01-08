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

import titan.tasks.TaskHandler;

public class PdfConversionHandler implements TaskHandler {
    @Override
    public String execute(String payload){
        System.out.println("[INFO] [PDF WORKER] Converting file: " + payload);
        // Simulating heavy work
        try { Thread.sleep(3000); } catch (InterruptedException e) {}

        return "PDF_GENERATED_AT_/tmp/" + payload + ".pdf";
    }
}

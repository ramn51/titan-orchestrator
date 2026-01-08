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

package titan.network;

import titan.network.TitanProtocol;
import titan.scheduler.WorkerRegistry;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class RpcClient {
    WorkerRegistry workerRegistry;
//    public RpcClient(){}
    public RpcClient(WorkerRegistry workerRegistry){
        this.workerRegistry = workerRegistry;
    }

    public String sendRequest(String host, int port, String payload) {
        // Defaulting to OP_SUBMIT_JOB or generic OP_DATA for legacy calls
        return sendRequest(host, port, TitanProtocol.OP_SUBMIT_JOB, payload);
    }

    public String sendRequest(String host, int port, byte opCode, String payload){
        try(Socket socket = new Socket(host, port)){
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            int timeout = 30000;
            socket.setSoTimeout(timeout); // If i dont get response within 30 seconds I timeout (detecting failed jobs)
            TitanProtocol.send(out, opCode, payload);

            DataInputStream in = new DataInputStream(socket.getInputStream());
            TitanProtocol.TitanPacket response = TitanProtocol.read(in);

            if (response.opCode == TitanProtocol.OP_ERROR) {
                System.err.println("[RpcClient] Server returned error: " + response.payload);
                return "ERROR: " + response.payload;
            }
            return response.payload;
        }catch (IOException e) {
            System.err.println("[RpcClient] IO Error to " + host + ":" + port + " -> " + e.getMessage());
            return null; // Signals a dead worker/main.java.titan.network issue
        }
        catch (Exception e){
            e.printStackTrace();
            return "ERROR_CLIENT: " + e.getMessage();
        }
    }

}

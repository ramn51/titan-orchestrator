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

/**
 * {@code RpcClient} provides functionality for making Remote Procedure Calls (RPC) to a server.
 * It handles the underlying network communication, including connecting to a host and port,
 * sending requests using the {@link titan.network.TitanProtocol}, and receiving responses.
 * <p>
 * This client is designed to interact with servers that implement the {@link titan.network.TitanProtocol}
 * for data exchange, supporting various operation codes and payloads.
 * </p>
 */
    public class RpcClient {
    WorkerRegistry workerRegistry;
//    public RpcClient(){}
    /**
 * Constructs a new {@code RpcClient} instance.
 * <p>
 * This constructor initializes the client with a {@link WorkerRegistry},
 * which might be used for internal client-side worker management or lookup,
 * though its direct usage is not explicitly shown in the provided methods.
 * </p>
 * @param workerRegistry The {@link WorkerRegistry} to associate with this client.
 */
    public RpcClient(WorkerRegistry workerRegistry){
        this.workerRegistry = workerRegistry;
    }

    /**
 * Sends a request to a specified host and port with a default operation code.
 * <p>
 * This method defaults to using {@link TitanProtocol#OP_SUBMIT_JOB} as the operation code.
 * It is a convenience method for common operations like submitting jobs where the specific
 * operation code might be implicit or less critical than the payload itself.
 * </p>
 * @param host The hostname or IP address of the server to connect to.
 * @param port The port number on the server to connect to.
 * @param payload The string payload to send as part of the request.
 * @return The response payload from the server as a {@code String}, or an error message
 *         prefixed with "ERROR:" if the server returns an error, or {@code null} if an
 *         IO error occurs indicating a potential network issue or dead worker.
 * @see #sendRequest(String, int, byte, String)
 */
    public String sendRequest(String host, int port, String payload) {
        // Defaulting to OP_SUBMIT_JOB or generic OP_DATA for legacy calls
        return sendRequest(host, port, TitanProtocol.OP_SUBMIT_JOB, payload);
    }

    /**
 * Sends a request to a specified host and port with a given operation code and payload.
 * <p>
 * This is the primary method for sending RPC requests. It establishes a socket connection,
 * sets a read timeout, sends the request using {@link TitanProtocol#send(DataOutputStream, byte, String)},
 * and then waits for and reads the server's response using {@link TitanProtocol#read(DataInputStream)}.
 * </p>
 * <p>
 * If the server returns an error ({@link TitanProtocol#OP_ERROR}), an error message is logged
 * and returned. In case of an {@link IOException}, it indicates a network problem or a dead
 * server/worker, and {@code null} is returned. Other exceptions result in a client-side error message.
 * </p>
 * @param host The hostname or IP address of the server to connect to.
 * @param port The port number on the server to connect to.
 * @param opCode The operation code (e.g., {@link TitanProtocol#OP_SUBMIT_JOB}) indicating the type of request.
 * @param payload The string payload to send as part of the request.
 * @return The response payload from the server as a {@code String}, or an error message
 *         prefixed with "ERROR:" if the server returns an error, or {@code null} if an
 *         IO error occurs indicating a potential network issue or dead worker.
 */
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

package network;

import scheduler.WorkerRegistry;

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
            socket.setSoTimeout(10000); // If i dont get response within 3 seconds I timeout (detecting failed jobs)
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
            return null; // Signals a dead worker/network issue
        }
        catch (Exception e){
            e.printStackTrace();
            return "ERROR_CLIENT: " + e.getMessage();
        }
    }

}

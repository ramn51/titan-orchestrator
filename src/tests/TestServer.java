package tests;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import network.TitanProtocol;


public class TestServer {
    public static void main(String[] args){
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(()->{
            try (ServerSocket server = new ServerSocket(9999)) {
                System.out.println("Listening on 9999");

                try (Socket socket = server.accept()) {
                    // Read the request
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());

                    TitanProtocol.TitanPacket packet = TitanProtocol.read(in);
                    System.out.println("Message from client: " + packet.payload);

                    // FIX 2: Send with OpCode (Using OP_DATA = 0x52)
                    TitanProtocol.send(out, TitanProtocol.OP_DATA, "Echo: " + packet.payload + " SomeOf");
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        try { Thread.sleep(100); } catch (Exception e) {}



        try{
            Socket socket = new Socket("localhost", 9999);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            TitanProtocol.send(out, TitanProtocol.OP_DATA, "Testing TCP!");

            DataInputStream in = new DataInputStream(socket.getInputStream());

            TitanProtocol.TitanPacket response = TitanProtocol.read(in);
            System.out.println("[Client] Got back: " + response.payload);

        } catch (Exception e){
            e.printStackTrace();
        }

        executorService.shutdown();
    }
}

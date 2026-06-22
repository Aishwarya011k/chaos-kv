package com.chaoskv;

import java.io.*;
import java.net.Socket;

public class NodePeer {
    private final String host;
    private final int port;

    public NodePeer(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void replicate(String command) {
        try (
            Socket socket = new Socket(host, port);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            out.println("REPLICATE " + command);
        } catch (IOException e) {
            System.out.println("Peer " + host + ":" + port + " unreachable");
        }
    }
}

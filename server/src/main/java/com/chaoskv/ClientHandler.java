package com.chaoskv;

import java.io.*;
import java.net.Socket;
import java.util.List;

public class ClientHandler implements Runnable {
    private final Socket socket;
    private final KVStore store;
    private final List<NodePeer> peers;

    public ClientHandler(Socket socket, KVStore store, List<NodePeer> peers) {
        this.socket = socket;
        this.store = store;
        this.peers = peers;
    }

    @Override
    public void run() {
        try (
            BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] parts = line.trim().split("\\s+", 3);
                String cmd = parts[0].toUpperCase();

                if (cmd.equals("REPLICATE")) {
                    // incoming replication from leader — apply silently
                    String inner = line.substring("REPLICATE ".length());
                    String[] rParts = inner.trim().split("\\s+", 3);
                    switch (rParts[0].toUpperCase()) {
                        case "PUT"    -> store.put(rParts[1], rParts[2]);
                        case "DELETE" -> store.delete(rParts[1]);
                    }
                    out.println("ACK");
                    continue;
                }

                String response = switch (cmd) {
                    case "PUT" -> {
                        if (parts.length < 3) yield "ERR usage: PUT key value";
                        String result = store.put(parts[1], parts[2]);
                        peers.forEach(p -> p.replicate("PUT " + parts[1] + " " + parts[2]));
                        yield result;
                    }
                    case "GET" -> parts.length == 2
                                  ? store.get(parts[1])
                                  : "ERR usage: GET key";
                    case "DELETE" -> {
                        if (parts.length < 2) yield "ERR usage: DELETE key";
                        String result = store.delete(parts[1]);
                        peers.forEach(p -> p.replicate("DELETE " + parts[1]));
                        yield result;
                    }
                    default -> "ERR unknown command";
                };
                out.println(response);
            }
        } catch (IOException e) {
            System.out.println("Client disconnected: " + e.getMessage());
        }
    }
}

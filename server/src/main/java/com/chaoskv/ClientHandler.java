package com.chaoskv;

import java.io.*;
import java.net.Socket;
import java.util.List;

public class ClientHandler implements Runnable {
    private final Socket socket;
    private final KVStore store;
    private final List<NodePeer> peers;
    private final RaftNode raft;

    public ClientHandler(Socket socket, KVStore store,
                         List<NodePeer> peers, RaftNode raft) {
        this.socket = socket;
        this.store  = store;
        this.peers  = peers;
        this.raft   = raft;
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

                switch (cmd) {
                    case "HEARTBEAT" -> {
                        int term = Integer.parseInt(parts[1]);
                        String leaderId = parts[2];
                        raft.handleHeartbeat(term, leaderId);
                    }
                    case "VOTE_REQUEST" -> {
                        int term = Integer.parseInt(parts[1]);
                        String candidateId = parts[2];
                        out.println(raft.handleVoteRequest(term, candidateId));
                    }
                    case "REPLICATE" -> {
                        String inner = line.substring("REPLICATE ".length());
                        String[] rp = inner.trim().split("\\s+", 3);
                        switch (rp[0].toUpperCase()) {
                            case "PUT"    -> store.put(rp[1], rp[2]);
                            case "DELETE" -> store.delete(rp[1]);
                        }
                        out.println("ACK");
                    }
                    case "PUT" -> {
                        if (raft.getState() != RaftNode.State.LEADER) {
                            out.println("ERR not the leader");
                            break;
                        }
                        if (parts.length < 3) { out.println("ERR usage: PUT key value"); break; }
                        String res = store.put(parts[1], parts[2]);
                        peers.forEach(p -> p.replicate("PUT " + parts[1] + " " + parts[2]));
                        out.println(res);
                    }
                    case "GET" -> {
                        if (parts.length < 2) { out.println("ERR usage: GET key"); break; }
                        out.println(store.get(parts[1]));
                    }
                    case "DELETE" -> {
                        if (raft.getState() != RaftNode.State.LEADER) {
                            out.println("ERR not the leader");
                            break;
                        }
                        if (parts.length < 2) { out.println("ERR usage: DELETE key"); break; }
                        String res = store.delete(parts[1]);
                        peers.forEach(p -> p.replicate("DELETE " + parts[1]));
                        out.println(res);
                    }
                    case "STATUS" -> out.println(
                        "node=" + raft.getNodeId()
                        + " state=" + raft.getState()
                        + " term=" + raft.getTerm()
                    );
                    case "KILL" -> {
                        out.println("DYING");
                        out.flush();
                        new Thread(() -> {
                            try { Thread.sleep(200); } catch (InterruptedException ignored) {}
                            System.exit(0);
                        }).start();
                    }
                    default -> out.println("ERR unknown command");
                }
            }
        } catch (IOException e) {
            System.out.println("Client disconnected: " + e.getMessage());
        }
    }
}

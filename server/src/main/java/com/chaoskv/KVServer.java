package com.chaoskv;

import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class KVServer {
    private static final int THREAD_POOL_SIZE = 10;

    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8001;
        String nodeId = "node-" + port;

        WriteAheadLog wal = new WriteAheadLog(nodeId);
        KVStore store = new KVStore(wal);
        wal.replay(store);

        List<String> peerAddresses = new ArrayList<>();
        List<NodePeer> peers = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            peerAddresses.add(args[i]);
            String[] hp = args[i].split(":");
            peers.add(new NodePeer(hp[0], Integer.parseInt(hp[1])));
        }

        int dashboardPort = port + 1000;
        DashboardServer dashboard = new DashboardServer(dashboardPort);
        dashboard.start();

        RaftNode raft = new RaftNode(nodeId, port, peerAddresses, dashboard);
        raft.start();

        ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        System.out.println("[" + nodeId + "] server ready on port " + port
                           + " | dashboard ws on port " + dashboardPort);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (true) {
                Socket client = serverSocket.accept();
                pool.submit(new ClientHandler(client, store, peers, raft));
            }
        }
    }
}

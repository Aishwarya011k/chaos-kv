package com.chaoskv;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class RaftNode {
    public enum State { FOLLOWER, CANDIDATE, LEADER }

    private final String nodeId;
    private final int port;
    private final List<String> peerAddresses;
    private final DashboardServer dashboard; // can be null

    private volatile State state = State.FOLLOWER;
    private final AtomicInteger currentTerm = new AtomicInteger(0);
    private volatile String votedFor = null;
    private volatile long lastHeartbeat = System.currentTimeMillis();

    private final int ELECTION_TIMEOUT_MIN = 150;
    private final int ELECTION_TIMEOUT_MAX = 300;
    private final int HEARTBEAT_INTERVAL = 100;

    private final ScheduledExecutorService scheduler =
        Executors.newScheduledThreadPool(2);

    public RaftNode(String nodeId, int port, List<String> peerAddresses, DashboardServer dashboard) {
        this.nodeId = nodeId;
        this.port = port;
        this.peerAddresses = peerAddresses;
        this.dashboard = dashboard;
    }

    public void start() {
        System.out.println("[" + nodeId + "] starting as FOLLOWER, term=0");
        broadcastState();
        startElectionTimer();
    }

    private void broadcastState() {
        if (dashboard == null) return;
        String json = String.format(
            "{\"nodeId\":\"%s\",\"state\":\"%s\",\"term\":%d,\"ts\":%d}",
            nodeId, state, currentTerm.get(), System.currentTimeMillis()
        );
        dashboard.broadcast(json);
    }

    private void startElectionTimer() {
        int timeout = ELECTION_TIMEOUT_MIN +
            new Random().nextInt(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN);

        scheduler.scheduleAtFixedRate(() -> {
            long elapsed = System.currentTimeMillis() - lastHeartbeat;
            if (state != State.LEADER && elapsed > timeout) {
                startElection();
            }
        }, timeout, timeout, TimeUnit.MILLISECONDS);
    }

    private void startElection() {
        state = State.CANDIDATE;
        int term = currentTerm.incrementAndGet();
        votedFor = nodeId;
        int votes = 1;
        broadcastState();

        System.out.println("[" + nodeId + "] starting election for term=" + term);

        for (String peer : peerAddresses) {
            try {
                String[] hp = peer.split(":");
                Socket socket = new Socket(hp[0], Integer.parseInt(hp[1]));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()));

                out.println("VOTE_REQUEST " + term + " " + nodeId);
                String response = in.readLine();
                socket.close();

                if ("VOTE_GRANTED".equals(response)) {
                    votes++;
                    System.out.println("[" + nodeId + "] got vote from " + peer);
                }
            } catch (IOException e) {
                System.out.println("[" + nodeId + "] peer " + peer + " unreachable");
            }
        }

        int majority = (peerAddresses.size() + 1) / 2 + 1;
        if (votes >= majority) {
            becomeLeader();
        } else {
            state = State.FOLLOWER;
            broadcastState();
            System.out.println("[" + nodeId + "] lost election, back to FOLLOWER");
        }
    }

    private void becomeLeader() {
        state = State.LEADER;
        broadcastState();
        System.out.println("[" + nodeId + "] became LEADER for term="
                           + currentTerm.get());

        scheduler.scheduleAtFixedRate(() -> {
            if (state == State.LEADER) sendHeartbeats();
        }, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeats() {
        for (String peer : peerAddresses) {
            try {
                String[] hp = peer.split(":");
                Socket socket = new Socket(hp[0], Integer.parseInt(hp[1]));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                out.println("HEARTBEAT " + currentTerm.get() + " " + nodeId);
                socket.close();
            } catch (IOException e) {
                // peer down — ignore
            }
        }
    }

    public String handleVoteRequest(int term, String candidateId) {
        if (term > currentTerm.get()) {
            currentTerm.set(term);
            state = State.FOLLOWER;
            votedFor = null;
        }
        if (term >= currentTerm.get() && (votedFor == null || votedFor.equals(candidateId))) {
            votedFor = candidateId;
            lastHeartbeat = System.currentTimeMillis();
            System.out.println("[" + nodeId + "] voted for " + candidateId
                               + " in term=" + term);
            return "VOTE_GRANTED";
        }
        return "VOTE_DENIED";
    }

    public void handleHeartbeat(int term, String leaderId) {
        if (term >= currentTerm.get()) {
            currentTerm.set(term);
            if (state != State.FOLLOWER) {
                state = State.FOLLOWER;
                broadcastState();
            }
            lastHeartbeat = System.currentTimeMillis();
        }
    }

    public State getState()       { return state; }
    public int getTerm()          { return currentTerm.get(); }
    public String getNodeId()     { return nodeId; }
}

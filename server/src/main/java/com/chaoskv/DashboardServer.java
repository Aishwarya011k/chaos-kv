package com.chaoskv;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.util.concurrent.CopyOnWriteArraySet;

public class DashboardServer extends WebSocketServer {

    private final CopyOnWriteArraySet<WebSocket> clients = new CopyOnWriteArraySet<>();

    public DashboardServer(int port) {
        super(new InetSocketAddress(port));
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        clients.add(conn);
        System.out.println("Dashboard client connected: " + conn.getRemoteSocketAddress());
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        clients.remove(conn);
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        // dashboard doesn't send commands to us, ignore
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.out.println("Dashboard WS error: " + ex.getMessage());
    }

    @Override
    public void onStart() {
        System.out.println("Dashboard WebSocket server started");
    }

    // call this from RaftNode / KVStore whenever something changes
    public void broadcast(String jsonEvent) {
        for (WebSocket client : clients) {
            if (client.isOpen()) {
                client.send(jsonEvent);
            }
        }
    }
}

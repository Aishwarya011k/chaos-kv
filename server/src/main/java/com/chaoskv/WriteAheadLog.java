package com.chaoskv;

import java.io.*;
import java.nio.file.*;

public class WriteAheadLog {
    private final String logPath;

    public WriteAheadLog(String nodeId) {
        this.logPath = "wal-" + nodeId + ".log";
    }

    public synchronized void append(String command) {
        try (FileWriter fw = new FileWriter(logPath, true)) {
            fw.write(command + "\n");
        } catch (IOException e) {
            System.out.println("WAL write error: " + e.getMessage());
        }
    }

    public void replay(KVStore store) {
        File file = new File(logPath);
        if (!file.exists()) return;

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.trim().split("\\s+", 3);
                switch (parts[0].toUpperCase()) {
                    case "PUT"    -> store.put(parts[1], parts[2]);
                    case "DELETE" -> store.delete(parts[1]);
                }
            }
            System.out.println("WAL replayed from " + logPath);
        } catch (IOException e) {
            System.out.println("WAL replay error: " + e.getMessage());
        }
    }
}

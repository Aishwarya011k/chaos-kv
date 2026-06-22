package com.chaoskv;

import java.util.concurrent.ConcurrentHashMap;

public class KVStore {
    private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
    private final WriteAheadLog wal;

    public KVStore(WriteAheadLog wal) {
        this.wal = wal;
    }

    public String put(String key, String value) {
        wal.append("PUT " + key + " " + value);
        store.put(key, value);
        return "OK";
    }

    public String get(String key) {
        String val = store.get(key);
        return val != null ? val : "NULL";
    }

    public String delete(String key) {
        wal.append("DELETE " + key);
        return store.remove(key) != null ? "OK" : "NOT_FOUND";
    }
}

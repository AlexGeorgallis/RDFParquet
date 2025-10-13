package org.example.dictionary;

import java.util.*;

public class DictionaryEncoder {
    private static final DictionaryEncoder instance = new DictionaryEncoder();

    private Map<String, Integer> encodeMap;
    private Map<Integer, String> decodeMap;
    private int currentId;

    private DictionaryEncoder() {
        this.encodeMap = new HashMap<>();
        this.decodeMap = new HashMap<>();
        this.currentId = 1;
    }

    public static DictionaryEncoder getInstance() {
        return instance;
    }

    public void init(int expectedEntries) {
        int capacity = (int) Math.ceil(expectedEntries / 0.75f) + 1;
        this.encodeMap = new HashMap<>(capacity, 0.75f);
        this.decodeMap = new HashMap<>(capacity, 0.75f);
        this.currentId = 1;
    }

    public int encode(String value) {
        return encodeMap.computeIfAbsent(value, v -> {
            int id = currentId++;
            decodeMap.put(id, v);
            return id;
        });
    }

    public String decode(int id) {
        return decodeMap.get(id);
    }

    public Map<String,Integer> getEncodeMap() {
        return encodeMap;
    }

    public Map<Integer,String> getDecodeMap() {
        return decodeMap;
    }

    public List<DictionaryEntry> getEntries() {
        List<DictionaryEntry> entries = new ArrayList<>(decodeMap.size());
        for (Map.Entry<Integer,String> e : decodeMap.entrySet()) {
            entries.add(new DictionaryEntry(e.getKey(), e.getValue()));
        }
        return entries;
    }

    public void loadFrom(List<DictionaryEntry> entries) {
        encodeMap.clear();
        decodeMap.clear();
        currentId = 1;
        for (DictionaryEntry entry : entries) {
            encodeMap.put(entry.getValue(), entry.getId());
            decodeMap.put(entry.getId(), entry.getValue());
            currentId = Math.max(currentId, entry.getId() + 1);
        }
    }

}

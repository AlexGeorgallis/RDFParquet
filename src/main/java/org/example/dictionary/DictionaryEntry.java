package org.example.dictionary;

public class DictionaryEntry {

    private final int id;
    private final String value;

    public DictionaryEntry(int id, String value) {
        this.id = id;
        this.value = value;
    }

    public int getId() {
        return id;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return id + ": " + value;
    }

}

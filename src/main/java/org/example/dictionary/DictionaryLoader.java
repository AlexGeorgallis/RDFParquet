package org.example.dictionary;

import java.io.IOException;
import java.util.List;

public class DictionaryLoader {

    public static void load(String dictionaryPath) throws IOException {
        List<DictionaryEntry> entries = DictionaryParquetReader.readEntries(dictionaryPath);
        DictionaryEncoder.getInstance().loadFrom(entries);
    }
}

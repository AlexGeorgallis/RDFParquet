package org.example.dictionary;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DictionaryParquetReader {

    public static List<DictionaryEntry> readEntries(String parquetPath) throws IOException {
        List<DictionaryEntry> entries = new ArrayList<>();

        Path path = new Path(parquetPath);
        try (ParquetReader<DictionaryEntry> reader = ParquetReader
                .builder(new DictionaryReadSupport(), path)
                .build()) {
            DictionaryEntry entry;
            while ((entry = reader.read()) != null) {
                entries.add(entry);
            }
        }
        return entries;
    }

    public static ParquetReader<DictionaryEntry> create(Path path, FilterCompat.Filter filter) throws IOException {
        return ParquetReader.builder(new DictionaryReadSupport(), path)
//                .withFilter(filter)
                .useDictionaryFilter(true)
                .useBloomFilter(true)
                .build();
    }

}

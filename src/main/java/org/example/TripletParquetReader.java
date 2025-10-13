package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;

public class TripletParquetReader {

    // Read with filter
    public static ParquetReader<Triplet> create(Path path, FilterCompat.Filter filter) throws IOException {
        return ParquetReader.builder(new TripletReadSupport(), path)
                .withFilter(filter)
                .useDictionaryFilter(true)
                .useBloomFilter(true)
                .useStatsFilter(true)
                .withConf(new Configuration())
                .build();
    }

    // Read without filter
    public static ParquetReader<Triplet> create(Path path) throws IOException {
        return ParquetReader.builder(new TripletReadSupport(), path)
                .withConf(new Configuration())
                .build();
    }

}

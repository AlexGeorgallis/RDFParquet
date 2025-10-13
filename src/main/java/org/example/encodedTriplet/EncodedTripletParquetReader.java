package org.example.encodedTriplet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetReader;


import java.io.IOException;

public class EncodedTripletParquetReader {

    public static ParquetReader<EncodedTriplet> create(Path path, FilterCompat.Filter filter) throws IOException {
        Configuration conf = new Configuration();

        return ParquetReader.builder(new EncodedTripletReadSupport(), path)
                .withConf(conf)
                .withFilter(filter)
                .useBloomFilter(true)
                .useStatsFilter(true)
                .build();
    }


}

package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.example.dictionary.MainDictionary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TripletParquetWriter {

    public static ParquetWriter<Triplet> create(Path path, MessageType schema, CompressionCodecName compressionCodecName) throws IOException {
        return new Builder(path, schema)
                .withCompressionCodec(compressionCodecName)
                .withRowGroupSize((long) ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withConf(new Configuration())
                .withDictionaryEncoding("subject", true)
                .withDictionaryEncoding("object", true)
                .withDictionaryEncoding("predicate", true)
                .withBloomFilterEnabled("subject", true)
                .withBloomFilterEnabled("predicate", true)
                .withBloomFilterEnabled("object", true)
                .build();
    }

    public static class Builder extends ParquetWriter.Builder<Triplet, Builder> {
        private final MessageType schema;

        public Builder(Path path, MessageType schema) {
            super(path);
            this.schema = schema;
        }

        @Override
        protected WriteSupport<Triplet> getWriteSupport(Configuration conf) {
            return new TripletWriteSupport(schema);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }

}

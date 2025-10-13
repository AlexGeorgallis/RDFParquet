package org.example.encodedTriplet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

public class EncodedTripletParquetWriter {

    public static ParquetWriter<EncodedTriplet> create(Path path, MessageType schema, CompressionCodecName codec) throws IOException {

        Configuration writeConf = new Configuration();
        return new Builder(path, schema)
                .withConf(writeConf)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withCompressionCodec(codec)
                .withRowGroupSize((long)ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withBloomFilterEnabled("subject", true)
                .withBloomFilterEnabled("predicate",  true)
                .withBloomFilterEnabled("object",  true)
                .withBloomFilterFPP("subject", 0.001)
                .withBloomFilterFPP("object",  0.001)
                .withBloomFilterFPP("predicate", 0.001)
                .withDictionaryEncoding(true)
                .withSizeStatisticsEnabled(true)
                .withStatisticsEnabled(true)
                .build();
    }

    public static class Builder extends ParquetWriter.Builder<EncodedTriplet, Builder> {
        private final MessageType schema;

        public Builder(Path path, MessageType schema) {
            super(path);
            this.schema = schema;
        }

        @Override
        protected WriteSupport<EncodedTriplet> getWriteSupport(Configuration conf) {
            return new EncodedTripletWriteSupport(schema);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}

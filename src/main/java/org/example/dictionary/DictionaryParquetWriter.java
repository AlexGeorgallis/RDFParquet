package org.example.dictionary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

public class DictionaryParquetWriter {

    public static ParquetWriter<DictionaryEntry> create(Path path, MessageType schema, CompressionCodecName codec) throws IOException {
        return new Builder(path, schema)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withCompressionCodec(codec)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withRowGroupSize((long)ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withDictionaryEncoding("value", true)
                .build();
    }

    private static class Builder extends ParquetWriter.Builder<DictionaryEntry, Builder> {
        private final MessageType schema;

        public Builder(Path path, MessageType schema) {
            super(path);
            this.schema = schema;
        }

        @Override
        protected WriteSupport<DictionaryEntry> getWriteSupport(Configuration configuration) {
            return new DictionaryWriteSupport(schema);
        }

        @Override
        public Builder self() {
            return this;
        }
    }
}

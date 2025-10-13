package org.example.dictionary;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.util.Map;

public class DictionaryReadSupport extends ReadSupport<DictionaryEntry> {

    @Override
    public ReadContext init(InitContext context) {
        return new ReadContext(context.getFileSchema());
    }

    @Override
    public RecordMaterializer<DictionaryEntry> prepareForRead(
            org.apache.hadoop.conf.Configuration configuration,
            Map<String, String> keyValueMetaData,
            MessageType fileSchema,
            ReadContext readContext) {
        return new DictionaryMaterializer();
    }
}

package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

public class TripletReadSupport extends ReadSupport<Triplet> {

    @Override
    public ReadContext init(InitContext context) {
        return new ReadContext(context.getFileSchema());
    }

    @Override
    public RecordMaterializer<Triplet> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
        return new TripletMaterializer();
    }
}

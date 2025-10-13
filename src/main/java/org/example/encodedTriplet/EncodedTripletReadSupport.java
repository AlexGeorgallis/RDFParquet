package org.example.encodedTriplet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

public class EncodedTripletReadSupport extends ReadSupport<EncodedTriplet> {
    @Override
    public ReadContext init(InitContext context) {
        MessageType schema = context.getFileSchema();
        return new ReadContext(schema);
    }

    @Override
    public RecordMaterializer<EncodedTriplet> prepareForRead(
            Configuration configuration,
            Map<String, String> keyValueMetaData,
            MessageType fileSchema,
            ReadContext readContext) {
        return new EncodedTripletMaterializer();
    }
}

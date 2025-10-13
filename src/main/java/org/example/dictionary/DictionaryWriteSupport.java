package org.example.dictionary;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;

public class DictionaryWriteSupport extends WriteSupport<DictionaryEntry> {
    private final MessageType schema;
    private RecordConsumer recordConsumer;

    public DictionaryWriteSupport(MessageType schema) {
        this.schema = schema;
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(schema, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(DictionaryEntry entry) {
        recordConsumer.startMessage();

        recordConsumer.startField("id", 0);
        recordConsumer.addInteger(entry.getId());
        recordConsumer.endField("id", 0);

        recordConsumer.startField("value", 1);
        recordConsumer.addBinary(Binary.fromString(entry.getValue()));
        recordConsumer.endField("value", 1);

        recordConsumer.endMessage();
    }
}

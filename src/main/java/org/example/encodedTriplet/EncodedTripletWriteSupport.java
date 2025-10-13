package org.example.encodedTriplet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

public class EncodedTripletWriteSupport extends WriteSupport<EncodedTriplet> {

    private final MessageType schema;
    private RecordConsumer recordConsumer;

    public EncodedTripletWriteSupport(MessageType schema) {
        this.schema = schema;
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(schema, Map.of());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(EncodedTriplet encodedTriplet) {
        recordConsumer.startMessage();

        recordConsumer.startField("subject", 0);
        recordConsumer.addInteger(encodedTriplet.getSubject());
        recordConsumer.endField("subject", 0);

        recordConsumer.startField("predicate", 1);
        recordConsumer.addInteger(encodedTriplet.getPredicate());
        recordConsumer.endField("predicate", 1);

        recordConsumer.startField("object", 2);
        recordConsumer.addInteger(encodedTriplet.getObject());
        recordConsumer.endField("object", 2);

        recordConsumer.endMessage();
    }
}

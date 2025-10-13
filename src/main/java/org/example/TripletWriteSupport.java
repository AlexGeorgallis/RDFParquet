package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;

public class TripletWriteSupport extends WriteSupport<Triplet> {
    private final MessageType schema;
    private RecordConsumer recordConsumer;

    public TripletWriteSupport(MessageType schema) {
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
    public void write(Triplet triplet) {
        recordConsumer.startMessage();

        recordConsumer.startField("subject", 0);
        recordConsumer.addBinary(org.apache.parquet.io.api.Binary.fromString(triplet.getSubject()));
        recordConsumer.endField("subject", 0);

        recordConsumer.startField("predicate", 1);
        recordConsumer.addBinary(org.apache.parquet.io.api.Binary.fromString(triplet.getPredicate()));
        recordConsumer.endField("predicate", 1);

        recordConsumer.startField("object", 2);
        recordConsumer.addBinary(org.apache.parquet.io.api.Binary.fromString(triplet.getObject()));
        recordConsumer.endField("object", 2);

        recordConsumer.endMessage();
    }

}

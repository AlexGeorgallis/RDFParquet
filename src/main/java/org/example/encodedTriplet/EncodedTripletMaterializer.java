package org.example.encodedTriplet;

import org.apache.parquet.io.api.*;

public class EncodedTripletMaterializer extends RecordMaterializer<EncodedTriplet> {

    private final EncodedTriplet reusable = new EncodedTriplet(0, 0, 0);

    private final int[] fields = new int[3];

    // three converters, one per column index
    private final PrimitiveConverter subjectConv = new PrimitiveConverter() {
        @Override public void addInt(int value) { fields[0] = value; }
    };
    private final PrimitiveConverter predicateConv = new PrimitiveConverter() {
        @Override public void addInt(int value) { fields[1] = value; }
    };
    private final PrimitiveConverter objectConv = new PrimitiveConverter() {
        @Override public void addInt(int value) { fields[2] = value; }
    };

    private final Converter[] converters = new Converter[] {
            subjectConv, predicateConv, objectConv
    };

    private final GroupConverter rootConverter = new GroupConverter() {
        @Override
        public Converter getConverter(int fieldIndex) {
            return converters[fieldIndex];
        }
        @Override public void start() { /* nothing */ }
        @Override public void end() {
            reusable.setSubject  (fields[0]);
            reusable.setPredicate(fields[1]);
            reusable.setObject   (fields[2]);
        }
    };

    @Override
    public GroupConverter getRootConverter() {
        return rootConverter;
    }

    @Override
    public EncodedTriplet getCurrentRecord() {
        return reusable;
    }

    @Override
    public void skipCurrentRecord() {
        // no-op
    }
}

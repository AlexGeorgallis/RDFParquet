package org.example;

import org.apache.parquet.io.api.*;

import java.io.IOException;

public class TripletMaterializer extends RecordMaterializer<Triplet> {

    private String subject;
    private String predicate;
    private String object;

    @Override
    public Triplet getCurrentRecord() {
        return new Triplet(subject, predicate, object);
    }

    @Override
    public GroupConverter getRootConverter() {
        return new GroupConverter() {

            @Override
            public void start() {
                subject = null;
                predicate = null;
                object = null;
            }

            @Override
            public void end() {
                //
            }

            @Override
            public Converter getConverter(int fieldIndex) {
                return new PrimitiveConverter() {
                    @Override
                    public void addBinary(Binary value) {
                        switch (fieldIndex) {
                            case 0: subject = value.toStringUsingUTF8(); break;
                            case 1: predicate = value.toStringUsingUTF8(); break;
                            case 2: object = value.toStringUsingUTF8(); break;
                            default: throw new IllegalArgumentException("Uknown field index: " + fieldIndex);
                        }
                    }
                };
            }
        };
    }
}



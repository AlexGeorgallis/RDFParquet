package org.example.dictionary;

import org.apache.parquet.io.api.*;

public class DictionaryMaterializer extends RecordMaterializer<DictionaryEntry> {

    private int idCurrent;
    private String valueCurrent;

    private final PrimitiveConverter idConverter = new PrimitiveConverter() {
        @Override
        public void addInt(int value) {
            idCurrent = value;
        }
    };

    private final PrimitiveConverter valueConverter = new PrimitiveConverter() {
        @Override
        public void addBinary(Binary binary) {
            valueCurrent = binary.toStringUsingUTF8();
        }
    };

    private final GroupConverter rootConverter = new GroupConverter() {
        @Override
        public void start() {
            idCurrent = -1;
            valueCurrent = null;
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return (fieldIndex == 0) ? idConverter : valueConverter;
        }

        @Override
        public void end() {
            // nothing
        }
    };

    @Override
    public GroupConverter getRootConverter() {
        return rootConverter;
    }

    @Override
    public DictionaryEntry getCurrentRecord() {
        return new DictionaryEntry(idCurrent, valueCurrent);
    }
}

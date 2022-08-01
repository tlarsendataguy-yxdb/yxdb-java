package com.tlarsendataguy.yxdb;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Array;
import java.util.*;
import java.util.function.Function;

public class YxdbRecord {
    private YxdbRecord(int fieldCount){
        nameToIndex = new HashMap<>(fieldCount);
        fields = new ArrayList<>(fieldCount);
        boolExtractors = new ArrayList<>(fieldCount);
        byteExtractors = new ArrayList<>(fieldCount);
        longExtractors = new ArrayList<>(fieldCount);
        doubleExtractors = new ArrayList<>(fieldCount);
        stringExtractors = new ArrayList<>(fieldCount);
        dateExtractors = new ArrayList<>(fieldCount);
        blobExtractors = new ArrayList<>(fieldCount);
    }
    public final List<YxdbField> fields;
    private ByteBuffer buffer;
    private final Map<String, Integer> nameToIndex;
    private final List<Function<ByteBuffer,Boolean>> boolExtractors;
    private final List<Function<ByteBuffer,Byte>> byteExtractors;
    private final List<Function<ByteBuffer,Long>> longExtractors;
    private final List<Function<ByteBuffer,Double>> doubleExtractors;
    private final List<Function<ByteBuffer,String>> stringExtractors;
    private final List<Function<ByteBuffer,Date>> dateExtractors;
    private final List<Function<ByteBuffer,byte[]>> blobExtractors;


    static YxdbRecord newFromFieldList(List<MetaInfoField> fields) throws IllegalArgumentException {
        YxdbRecord record = new YxdbRecord(fields.size());
        int startAt = 0;
        int varFields = 0;
        for (MetaInfoField field: fields) {
            switch (field.type()) {
                case "Int16":
                    record.addLongExtractor(field.name(), Extractors.NewInt16Extractor(startAt));
                    startAt += 3;
                    break;
                case "Int32":
                    record.addLongExtractor(field.name(), Extractors.NewInt32Extractor(startAt));
                    startAt += 5;
                    break;
                case "Int64":
                    record.addLongExtractor(field.name(), Extractors.NewInt64Extractor(startAt));
                    startAt += 9;
                    break;
                case "Float":
                    record.addDoubleExtractor(field.name(), Extractors.NewFloatExtractor(startAt));
                    startAt += 5;
            }
        }
        if (varFields > 0) {
            startAt += 4;
        }
        return record;
    }

    void loadRecordBlobFrom(byte[] sourceData, int start, int to) {
        if (to <= start) {
            throw new IllegalArgumentException("to must be greater than start");
        }
        var length = to - start;
        if (buffer == null || buffer.capacity() < length) {
            buffer = ByteBuffer.allocate(length+100).order(ByteOrder.LITTLE_ENDIAN);
        }
        System.arraycopy(sourceData, start, buffer.array(), 0, length);
    }

    public Long extractLongFrom(int index) {
        return longExtractors.get(index).apply(buffer);
    }

    public Long extractLongFrom(String name) {
        var index = nameToIndex.get(name);
        return extractLongFrom(index);
    }

    public Double extractDoubleFrom(int index) {
        return doubleExtractors.get(index).apply(buffer);
    }

    public Double extractDoubleFrom(String name) {
        var index = nameToIndex.get(name);
        return extractDoubleFrom(index);
    }

    private void addLongExtractor(String name, Function<ByteBuffer, Long> extractor) {
        boolExtractors.add(null);
        byteExtractors.add(null);
        longExtractors.add(extractor);
        doubleExtractors.add(null);
        stringExtractors.add(null);
        dateExtractors.add(null);
        blobExtractors.add(null);
        addFieldNameToIndexMap(name, Long.TYPE);
    }

    private void addDoubleExtractor(String name, Function<ByteBuffer, Double> extractor) {
        boolExtractors.add(null);
        byteExtractors.add(null);
        longExtractors.add(null);
        doubleExtractors.add(extractor);
        stringExtractors.add(null);
        dateExtractors.add(null);
        blobExtractors.add(null);
        addFieldNameToIndexMap(name, Double.TYPE);
    }

    private void addFieldNameToIndexMap(String name, Type type) {
        var index = fields.size();
        fields.add(new YxdbField(name, type));
        nameToIndex.put(name, index);
    }
}

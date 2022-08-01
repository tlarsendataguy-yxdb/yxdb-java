package com.tlarsendataguy.yxdb;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.function.Function;

public class YxdbRecord {
    private YxdbRecord(int fieldCount){
        nameToIndex = new HashMap<>(fieldCount);
        fields = new ArrayList<>(fieldCount);
        boolExtractors = new HashMap<>(fieldCount);
        byteExtractors = new HashMap<>(fieldCount);
        longExtractors = new HashMap<>(fieldCount);
        doubleExtractors = new HashMap<>(fieldCount);
        stringExtractors = new HashMap<>(fieldCount);
        dateExtractors = new HashMap<>(fieldCount);
        blobExtractors = new HashMap<>(fieldCount);
    }
    public final List<YxdbField> fields;
    private ByteBuffer buffer;
    private final Map<String, Integer> nameToIndex;
    private final Map<Integer, Function<ByteBuffer,Boolean>> boolExtractors;
    private final Map<Integer, Function<ByteBuffer,Byte>> byteExtractors;
    private final Map<Integer, Function<ByteBuffer,Long>> longExtractors;
    private final Map<Integer, Function<ByteBuffer,Double>> doubleExtractors;
    private final Map<Integer, Function<ByteBuffer,String>> stringExtractors;
    private final Map<Integer, Function<ByteBuffer,Date>> dateExtractors;
    private final Map<Integer, Function<ByteBuffer,byte[]>> blobExtractors;


    static YxdbRecord newFromFieldList(List<MetaInfoField> fields) throws IllegalArgumentException {
        YxdbRecord record = new YxdbRecord(fields.size());
        int startAt = 0;
        int varFields = 0;
        int size;
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
                    break;
                case "Double":
                    record.addDoubleExtractor(field.name(), Extractors.NewDoubleExtractor(startAt));
                    startAt += 9;
                    break;
                case "FixedDecimal":
                    size = field.size();
                    record.addDoubleExtractor(field.name(), Extractors.NewFixedDecimalExtractor(startAt, size));
                    startAt += size + 1;
                    break;
                case "String":
                    size = field.size();
                    record.addStringExtractor(field.name(), Extractors.NewStringExtractor(startAt, size));
                    startAt += size + 1;
                    break;
                case "WString":
                    size = field.size();
                    record.addStringExtractor(field.name(), Extractors.NewWStringExtractor(startAt, size));
                    startAt += (size * 2) + 1;
                    break;
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

    public String extractStringFrom(int index) {
        return stringExtractors.get(index).apply(buffer);
    }

    public String extractStringFrom(String name) {
        var index = nameToIndex.get(name);
        return extractStringFrom(index);
    }

    private void addLongExtractor(String name, Function<ByteBuffer, Long> extractor) {
        var index = addFieldNameToIndexMap(name, YxdbField.DataType.LONG);
        longExtractors.put(index, extractor);
    }

    private void addDoubleExtractor(String name, Function<ByteBuffer, Double> extractor) {
        var index = addFieldNameToIndexMap(name, YxdbField.DataType.DOUBLE);
        doubleExtractors.put(index, extractor);
    }

    private void addStringExtractor(String name, Function<ByteBuffer, String> extractor) {
        var index = addFieldNameToIndexMap(name, YxdbField.DataType.STRING);
        stringExtractors.put(index, extractor);

    }

    private int addFieldNameToIndexMap(String name, YxdbField.DataType type) {
        var index = fields.size();
        fields.add(new YxdbField(name, type));
        nameToIndex.put(name, index);
        return index;
    }
}

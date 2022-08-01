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
        boolExtractors = new ArrayList<>(fieldCount);
        byteExtractors = new ArrayList<>(fieldCount);
        longExtractors = new ArrayList<>(fieldCount);
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
    private final List<Function<ByteBuffer,String>> stringExtractors;
    private final List<Function<ByteBuffer,Date>> dateExtractors;
    private final List<Function<ByteBuffer,byte[]>> blobExtractors;


    public static YxdbRecord generateFrom(List<MetaInfoField> fields) throws IllegalArgumentException {
        YxdbRecord record = new YxdbRecord(fields.size());
        int startAt = 0;
        int startSize = 0;
        int varFields = 0;
        for (MetaInfoField field: fields) {
            switch (field.type()) {
                case "Int16":
                    record.addLongExtractor(field.name(), Extractors.NewInt16Extractor(startAt));
                    startSize += 3;
                    break;
                case "Int32":
                    record.addLongExtractor(field.name(), Extractors.NewInt32Extractor(startAt));
                    startSize += 5;
                    break;
            }
        }
        if (varFields > 0) {
            startSize += 4;
        }
        return record;
    }

    public void loadFrom(byte[] sourceData, int start, int to) {
        if (to <= start) {
            throw new IllegalArgumentException("to cannot be greater than start");
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

    private void addLongExtractor(String name, Function<ByteBuffer, Long> extractor) {
        boolExtractors.add(null);
        byteExtractors.add(null);
        longExtractors.add(extractor);
        stringExtractors.add(null);
        dateExtractors.add(null);
        blobExtractors.add(null);
        addFieldNameToIndexMap(name, Long.TYPE);
    }

    private void addFieldNameToIndexMap(String name, Type type) {
        var index = fields.size();
        fields.add(new YxdbField(name, type));
        nameToIndex.put(name, index);
    }
}

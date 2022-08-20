package com.tlarsendataguy.yxdb;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;

class YxdbRecord {
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
    public int fixedSize;
    public boolean hasVar;
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
        int size;
        for (MetaInfoField field: fields) {
            switch (field.type()) {
                case "Int16" -> {
                    record.addLongExtractor(field.name(), Extractors.NewInt16Extractor(startAt));
                    startAt += 3;
                }
                case "Int32" -> {
                    record.addLongExtractor(field.name(), Extractors.NewInt32Extractor(startAt));
                    startAt += 5;
                }
                case "Int64" -> {
                    record.addLongExtractor(field.name(), Extractors.NewInt64Extractor(startAt));
                    startAt += 9;
                }
                case "Float" -> {
                    record.addDoubleExtractor(field.name(), Extractors.NewFloatExtractor(startAt));
                    startAt += 5;
                }
                case "Double" -> {
                    record.addDoubleExtractor(field.name(), Extractors.NewDoubleExtractor(startAt));
                    startAt += 9;
                }
                case "FixedDecimal" -> {
                    size = field.size();
                    record.addDoubleExtractor(field.name(), Extractors.NewFixedDecimalExtractor(startAt, size));
                    startAt += size + 1;
                }
                case "String" -> {
                    size = field.size();
                    record.addStringExtractor(field.name(), Extractors.NewStringExtractor(startAt, size));
                    startAt += size + 1;
                }
                case "WString" -> {
                    size = field.size();
                    record.addStringExtractor(field.name(), Extractors.NewWStringExtractor(startAt, size));
                    startAt += (size * 2) + 1;
                }
                case "V_String" -> {
                    record.addStringExtractor(field.name(), Extractors.NewV_StringExtractor(startAt));
                    startAt += 4;
                    record.hasVar = true;
                }
                case "V_WString" -> {
                    record.addStringExtractor(field.name(), Extractors.NewV_WStringExtractor(startAt));
                    startAt += 4;
                    record.hasVar = true;
                }
                case "Date" -> {
                    record.addDateExtractor(field.name(), Extractors.NewDateExtractor(startAt));
                    startAt += 11;
                }
                case "DateTime" -> {
                    record.addDateExtractor(field.name(), Extractors.NewDateTimeExtractor(startAt));
                    startAt += 20;
                }
                case "Bool" -> {
                    record.addBooleanExtractor(field.name(), Extractors.NewBoolExtractor(startAt));
                    startAt++;
                }
                case "Byte" -> {
                    record.addByteExtractor(field.name(), Extractors.NewByteExtractor(startAt));
                    startAt += 2;
                }
                case "Blob", "SpatialObj" -> {
                    record.addBlobExtractor(field.name(), Extractors.NewBlobExtractor(startAt));
                    startAt += 4;
                    record.hasVar = true;
                }
                default -> throw new IllegalArgumentException("field type is invalid");
            }
        }
        record.fixedSize = startAt;
        return record;
    }

    public Long extractLongFrom(int index, ByteBuffer buffer) {
        var extractor = longExtractors.get(index);
        if (extractor == null) {
            throwInvalidIndex(index, "long integer");
        }
        return extractor.apply(buffer);
    }

    public Long extractLongFrom(String name, ByteBuffer buffer) {
        var index = nameToIndex.get(name);
        if (index == null) {
            throwInvalidName(name);
        }
        return extractLongFrom(index, buffer);
    }

    public Double extractDoubleFrom(int index, ByteBuffer buffer) {
        var extractor = doubleExtractors.get(index);
        if (extractor == null) {
            throwInvalidIndex(index, "numeric");
        }
        return extractor.apply(buffer);
    }

    public Double extractDoubleFrom(String name, ByteBuffer buffer) {
        var index = nameToIndex.get(name);
        if (index == null) {
            throwInvalidName(name);
        }
        return extractDoubleFrom(index, buffer);
    }

    public String extractStringFrom(int index, ByteBuffer buffer) {
        var extractor = stringExtractors.get(index);
        if (extractor == null) {
            throwInvalidIndex(index, "text");
        }
        return extractor.apply(buffer);
    }

    public String extractStringFrom(String name, ByteBuffer buffer) {
        var index = nameToIndex.get(name);
        if (index == null) {
            throwInvalidName(name);
        }
        return extractStringFrom(index, buffer);
    }

    public Date extractDateFrom(int index, ByteBuffer buffer) {
        var extractor = dateExtractors.get(index);
        if (extractor == null) {
            throwInvalidIndex(index, "date/datetime");
        }
        return extractor.apply(buffer);
    }

    public Date extractDateFrom(String name, ByteBuffer buffer) {
        var index = nameToIndex.get(name);
        if (index == null) {
            throwInvalidName(name);
        }
        return extractDateFrom(index, buffer);
    }

    public Boolean extractBooleanFrom(int index, ByteBuffer buffer) {
        var extractor = boolExtractors.get(index);
        if (extractor == null) {
            throwInvalidIndex(index, "boolean");
        }
        return extractor.apply(buffer);
    }

    public Boolean extractBooleanFrom(String name, ByteBuffer buffer) {
        var index = nameToIndex.get(name);
        if (index == null) {
            throwInvalidName(name);
        }
        return extractBooleanFrom(index, buffer);
    }

    public Byte extractByteFrom(int index, ByteBuffer buffer) {
        var extractor = byteExtractors.get(index);
        if (extractor == null) {
            throwInvalidIndex(index, "byte");
        }
        return extractor.apply(buffer);
    }

    public Byte extractByteFrom(String name, ByteBuffer buffer) {
        var index = nameToIndex.get(name);
        if (index == null) {
            throwInvalidName(name);
        }
        return extractByteFrom(index, buffer);
    }

    public byte[] extractBlobFrom(int index, ByteBuffer buffer) {
        var extractor = blobExtractors.get(index);
        if (extractor == null) {
            throwInvalidIndex(index, "blob");
        }
        return extractor.apply(buffer);
    }

    public byte[] extractBlobFrom(String name, ByteBuffer buffer) {
        var index = nameToIndex.get(name);
        if (index == null) {
            throwInvalidName(name);
        }
        return extractBlobFrom(index, buffer);
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

    private void addDateExtractor(String name, Function<ByteBuffer, Date> extractor) {
        var index = addFieldNameToIndexMap(name, YxdbField.DataType.DATE);
        dateExtractors.put(index, extractor);

    }

    private void addBooleanExtractor(String name, Function<ByteBuffer, Boolean> extractor) {
        var index = addFieldNameToIndexMap(name, YxdbField.DataType.BOOLEAN);
        boolExtractors.put(index, extractor);
    }

    private void addByteExtractor(String name, Function<ByteBuffer, Byte> extractor) {
        var index = addFieldNameToIndexMap(name, YxdbField.DataType.BYTE);
        byteExtractors.put(index, extractor);
    }

    private void addBlobExtractor(String name, Function<ByteBuffer, byte[]> extractor) {
        var index = addFieldNameToIndexMap(name, YxdbField.DataType.BLOB);
        blobExtractors.put(index, extractor);
    }

    private int addFieldNameToIndexMap(String name, YxdbField.DataType type) {
        var index = fields.size();
        fields.add(new YxdbField(name, type));
        nameToIndex.put(name, index);
        return index;
    }

    private static void throwInvalidIndex(int index, String expectedType) throws IllegalArgumentException {
        throw new IllegalArgumentException("index " + index + " is not a valid index or is not a " + expectedType + " field");
    }

    private static void throwInvalidName(String name) throws IllegalArgumentException {
        throw new IllegalArgumentException("field " + name + " does not exist");
    }
}

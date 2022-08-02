package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class YxdbRecordTest {
    @Test
    public void TestReadInt16Record() {
        var record = loadRecordWithValueColumn("Int16", 2);
        var source = wrap(new byte[]{23,0,0});

        Assertions.assertEquals(1, record.fields.size());
        Assertions.assertSame(YxdbField.DataType.LONG, record.fields.get(0).type());
        Assertions.assertEquals("value", record.fields.get(0).name());
        Assertions.assertEquals(23, record.extractLongFrom(0, source));
        Assertions.assertEquals(23, record.extractLongFrom("value", source));
        Assertions.assertEquals(3, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadInt32Record() {
        var record = loadRecordWithValueColumn("Int32",4);
        var source = wrap(new byte[]{23,0,0,0,0});

        Assertions.assertEquals(23, record.extractLongFrom(0, source));
        Assertions.assertEquals(5, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadInt64Record() {
        var record = loadRecordWithValueColumn("Int64", 8);
        var source = wrap(new byte[]{23,0,0,0,0,0,0,0,0});

        Assertions.assertEquals(23, record.extractLongFrom(0, source));
        Assertions.assertEquals(9, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadFloatRecord() {
        var record = loadRecordWithValueColumn("Float", 4);
        var source = wrap(new byte[]{-51,-52,-116,63,0,0,0,0, 0});

        Assertions.assertEquals(1, record.fields.size());
        Assertions.assertSame(YxdbField.DataType.DOUBLE, record.fields.get(0).type());
        Assertions.assertEquals("value", record.fields.get(0).name());
        Assertions.assertEquals(1.1f, record.extractDoubleFrom(0, source));
        Assertions.assertEquals(1.1f, record.extractDoubleFrom("value", source));
        Assertions.assertEquals(5, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadDoubleRecord() {
        var record = loadRecordWithValueColumn("Double", 8);
        var source = wrap(new byte[]{-102,-103,-103,-103,-103,-103,-15,63,0});

        Assertions.assertEquals(1.1, record.extractDoubleFrom(0, source));
        Assertions.assertEquals(9, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadFixedDecimalRecord() {
        var record = loadRecordWithValueColumn("FixedDecimal", 10);
        var source = wrap(new byte[]{49, 50, 51, 46, 52, 53, 0, 43, 67, 110, 0});

        Assertions.assertEquals(123.45, record.extractDoubleFrom(0, source));
        Assertions.assertEquals(11, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadStringRecord() {
        var record = loadRecordWithValueColumn("String", 15);
        var source = wrap(new byte[]{104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33, 0, 23, 77, 0});

        Assertions.assertEquals(1, record.fields.size());
        Assertions.assertSame(YxdbField.DataType.STRING, record.fields.get(0).type());
        Assertions.assertEquals("value", record.fields.get(0).name());
        Assertions.assertEquals("hello world!", record.extractStringFrom(0, source));
        Assertions.assertEquals("hello world!", record.extractStringFrom("value", source));
        Assertions.assertEquals(16, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadWString() {
        var record = loadRecordWithValueColumn("WString", 15);
        var source = wrap(new byte[]{104, 0, 101, 0, 108, 0, 108, 0, 111, 0, 32, 0, 119, 0, 111, 0, 114, 0, 108, 0, 100, 0, 33, 0, 0, 0, 23, 0, 77, 0, 0});

        Assertions.assertEquals("hello world!", record.extractStringFrom(0, source));
        Assertions.assertEquals(31, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadV_String() {
        var record = loadRecordWithValueColumn("V_String", 15);
        var source = wrap(new byte[]{0, 0, 0, 0, 4, 0, 0, 0, 1,2,3,4,5,6,7,8});

        Assertions.assertEquals("", record.extractStringFrom(0, source));
        Assertions.assertEquals(4, record.fixedSize);
        Assertions.assertTrue(record.hasVar);
    }

    @Test
    public void TestReadV_WString() {
        var record = loadRecordWithValueColumn("V_WString", 15);
        var source = wrap(new byte[]{0, 0, 0, 0, 4, 0, 0, 0, 1,2,3,4,5,6,7,8});

        Assertions.assertEquals("", record.extractStringFrom(0, source));
        Assertions.assertEquals(4, record.fixedSize);
        Assertions.assertTrue(record.hasVar);
    }

    @Test
    public void TestReadDate() throws ParseException {
        var record = loadRecordWithValueColumn("Date", 10);
        var source = wrap(new byte[]{50,48,50,49,45,48,49,45,48,49,0});

        var expected = new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01");

        Assertions.assertEquals(1, record.fields.size());
        Assertions.assertSame(YxdbField.DataType.DATE, record.fields.get(0).type());
        Assertions.assertEquals("value", record.fields.get(0).name());
        Assertions.assertEquals(expected, record.extractDateFrom(0, source));
        Assertions.assertEquals(expected, record.extractDateFrom("value", source));
        Assertions.assertEquals(11, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadDateTime() throws ParseException {
        var record = loadRecordWithValueColumn("DateTime", 19);
        var source = wrap(new byte[]{50,48,50,49,45,48,49,45,48,50,32,48,51,58,48,52,58,48,53,0});

        var expected = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2021-01-02 03:04:05");

        Assertions.assertEquals(expected, record.extractDateFrom(0, source));
        Assertions.assertEquals(20, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadBool() {
        var record = loadRecordWithValueColumn("Bool", 1);
        var source = wrap(new byte[]{1});

        Assertions.assertEquals(1, record.fields.size());
        Assertions.assertSame(YxdbField.DataType.BOOLEAN, record.fields.get(0).type());
        Assertions.assertEquals("value", record.fields.get(0).name());
        Assertions.assertTrue(record.extractBooleanFrom(0, source));
        Assertions.assertTrue(record.extractBooleanFrom("value", source));
        Assertions.assertEquals(1, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadByte() {
        var record = loadRecordWithValueColumn("Byte", 2);
        var source = wrap(new byte[]{23, 0});

        Assertions.assertEquals(1, record.fields.size());
        Assertions.assertSame(YxdbField.DataType.BYTE, record.fields.get(0).type());
        Assertions.assertEquals("value", record.fields.get(0).name());
        Assertions.assertEquals((byte)23, record.extractByteFrom(0, source));
        Assertions.assertEquals((byte)23, record.extractByteFrom("value", source));
        Assertions.assertEquals(2, record.fixedSize);
        Assertions.assertFalse(record.hasVar);
    }

    @Test
    public void TestReadBlob() {
        var record = loadRecordWithValueColumn("Blob", 100);
        var source = wrap(new byte[]{0, 0, 0, 0, 4, 0, 0, 0, 1,2,3,4,5,6,7,8});

        Assertions.assertEquals(1, record.fields.size());
        Assertions.assertSame(YxdbField.DataType.BLOB, record.fields.get(0).type());
        Assertions.assertEquals("value", record.fields.get(0).name());
        Assertions.assertArrayEquals(new byte[]{}, record.extractBlobFrom(0, source));
        Assertions.assertArrayEquals(new byte[]{}, record.extractBlobFrom("value", source));
        Assertions.assertEquals(4, record.fixedSize);
        Assertions.assertTrue(record.hasVar);
    }

    @Test
    public void TestReadSpatialObj() {
        var record = loadRecordWithValueColumn("SpatialObj", 100);
        var source = wrap(new byte[]{0, 0, 0, 0, 4, 0, 0, 0, 1,2,3,4,5,6,7,8});

        Assertions.assertEquals(1, record.fields.size());
        Assertions.assertSame(YxdbField.DataType.BLOB, record.fields.get(0).type());
        Assertions.assertEquals("value", record.fields.get(0).name());
        Assertions.assertArrayEquals(new byte[]{}, record.extractBlobFrom(0, source));
    }

    private static YxdbRecord loadRecordWithValueColumn(String type, int size) {
        var fields = new ArrayList<MetaInfoField>(1);
        fields.add(new MetaInfoField("value", type, size, 0));
        return YxdbRecord.newFromFieldList(fields);
    }

    private static ByteBuffer wrap(byte[] source) {
        return ByteBuffer.wrap(source).order(ByteOrder.LITTLE_ENDIAN);
    }
}

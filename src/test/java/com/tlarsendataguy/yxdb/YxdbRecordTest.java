package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class YxdbRecordTest {
    @Test
    public void TestReadInt16Record() {
        var record = loadRecordWithValueColumn("Int16", 2, new byte[]{23,0,0});

        Assertions.assertEquals(1, record.fields.size());
        Assertions.assertSame(YxdbField.DataType.LONG, record.fields.get(0).type());
        Assertions.assertEquals("value", record.fields.get(0).name());
        Assertions.assertEquals(23, record.extractLongFrom(0));
        Assertions.assertEquals(23, record.extractLongFrom("value"));
    }

    @Test
    public void TestReadInt32Record() {
        var record = loadRecordWithValueColumn("Int32",4, new byte[]{23,0,0,0,0});

        Assertions.assertEquals(23, record.extractLongFrom(0));
    }

    @Test
    public void TestReadInt64Record() {
        var record = loadRecordWithValueColumn("Int64", 8, new byte[]{23,0,0,0,0,0,0,0,0});

        Assertions.assertEquals(23, record.extractLongFrom(0));
    }

    @Test
    public void TestReadFloatRecord() {
        var record = loadRecordWithValueColumn("Float", 4, new byte[]{-51,-52,-116,63,0,0,0,0, 0});

        Assertions.assertEquals(1, record.fields.size());
        Assertions.assertSame(YxdbField.DataType.DOUBLE, record.fields.get(0).type());
        Assertions.assertEquals("value", record.fields.get(0).name());
        Assertions.assertEquals(1.1f, record.extractDoubleFrom(0));
        Assertions.assertEquals(1.1f, record.extractDoubleFrom("value"));
    }

    @Test
    public void TestReadDoubleRecord() {
        var record = loadRecordWithValueColumn("Double", 8, new byte[]{-102,-103,-103,-103,-103,-103,-15,63,0});

        Assertions.assertEquals(1.1, record.extractDoubleFrom(0));
    }

    @Test
    public void TestReadFixedDecimalRecord() {
        var record = loadRecordWithValueColumn("FixedDecimal", 10, new byte[]{49, 50, 51, 46, 52, 53, 0, 43, 67, 110, 0});

        Assertions.assertEquals(123.45, record.extractDoubleFrom(0));
    }

    @Test
    public void TestReadStringRecord() {
        var record = loadRecordWithValueColumn("String", 15, new byte[]{104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33, 0, 23, 77, 0});

        Assertions.assertEquals(1, record.fields.size());
        Assertions.assertSame(YxdbField.DataType.STRING, record.fields.get(0).type());
        Assertions.assertEquals("value", record.fields.get(0).name());
        Assertions.assertEquals("hello world!", record.extractStringFrom(0));
        Assertions.assertEquals("hello world!", record.extractStringFrom("value"));
    }

    @Test
    public void TestReadWString() {
        var record = loadRecordWithValueColumn("WString", 15, new byte[]{104, 0, 101, 0, 108, 0, 108, 0, 111, 0, 32, 0, 119, 0, 111, 0, 114, 0, 108, 0, 100, 0, 33, 0, 0, 0, 23, 0, 77, 0, 0});

        Assertions.assertEquals("hello world!", record.extractStringFrom(0));
    }

    @Test
    public void TestReadV_String() {
        var record = loadRecordWithValueColumn("V_String", 15, new byte[]{0, 0, 0, 0, 4, 0, 0, 0, 1,2,3,4,5,6,7,8});

        Assertions.assertEquals("", record.extractStringFrom(0));
    }

    @Test
    public void TestReadV_WString() {
        var record = loadRecordWithValueColumn("V_WString", 1, 15, new byte[]{0, 0, 0, 0, 0, 4, 0, 0, 0, 1,2,3,4,5,6,7,8});

        Assertions.assertEquals("", record.extractStringFrom(0));
    }

    @Test
    public void TestReadDate() throws ParseException {
        var record = loadRecordWithValueColumn("Date", 4, 10, new byte[]{0,0,0,0,50,48,50,49,45,48,49,45,48,49,0});

        Assertions.assertEquals(1, record.fields.size());
        Assertions.assertSame(YxdbField.DataType.DATE, record.fields.get(0).type());
        Assertions.assertEquals("value", record.fields.get(0).name());
        Assertions.assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01"), record.extractDateFrom(0));
        Assertions.assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01"), record.extractDateFrom("value"));
    }

    @Test
    public void TestReadDateTime() throws ParseException {
        var record = loadRecordWithValueColumn("DateTime", 4, 10, new byte[]{0,0,0,0,50,48,50,49,45,48,49,45,48,50,32,48,51,58,48,52,58,48,53,0});

        Assertions.assertEquals(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2021-01-02 03:04:05"), record.extractDateFrom(0));
    }

    @Test
    public void TestReadBool() {
        var record = loadRecordWithValueColumn("Bool", 1, new byte[]{1});

        Assertions.assertEquals(1, record.fields.size());
        Assertions.assertSame(YxdbField.DataType.BOOLEAN, record.fields.get(0).type());
        Assertions.assertEquals("value", record.fields.get(0).name());
        Assertions.assertTrue(record.extractBooleanFrom(0));
        Assertions.assertTrue(record.extractBooleanFrom("value"));
    }

    @Test
    public void TestReadByte() {
        var record = loadRecordWithValueColumn("Byte", 2, new byte[]{23, 0});

        Assertions.assertEquals(1, record.fields.size());
        Assertions.assertSame(YxdbField.DataType.BYTE, record.fields.get(0).type());
        Assertions.assertEquals("value", record.fields.get(0).name());
        Assertions.assertEquals((byte)23, record.extractByteFrom(0));
        Assertions.assertEquals((byte)23, record.extractByteFrom("value"));
    }

    private static YxdbRecord loadRecordWithValueColumn(String type, int size, byte[] sourceData) {
        var fields = new ArrayList<MetaInfoField>(1);
        fields.add(new MetaInfoField("value", type, size, 0));
        var record = YxdbRecord.newFromFieldList(fields);
        record.loadRecordBlobFrom(sourceData, 0, sourceData.length);
        return record;
    }

    private static YxdbRecord loadRecordWithValueColumn(String type, int startAt, int size, byte[] sourceData) {
        var fields = new ArrayList<MetaInfoField>(1);
        fields.add(new MetaInfoField("value", type, size, 0));
        var record = YxdbRecord.newFromFieldList(fields);
        record.loadRecordBlobFrom(sourceData, startAt, sourceData.length);
        return record;
    }
}

package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

    private static YxdbRecord loadRecordWithValueColumn(String type, int size, byte[] sourceData) {
        var fields = new ArrayList<MetaInfoField>(1);
        fields.add(new MetaInfoField("value", type, size, 0));
        var record = YxdbRecord.newFromFieldList(fields);
        record.loadRecordBlobFrom(sourceData, 0, sourceData.length);
        return record;
    }
}

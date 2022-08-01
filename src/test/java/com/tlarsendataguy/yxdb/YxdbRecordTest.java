package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

public class YxdbRecordTest {
    @Test
    public void TestReadInt16Record() {
        var record = loadRecordWithValueColumn("Int16", 2, new byte[]{23,0,0});

        Assertions.assertEquals(1, record.fields.size());
        Assertions.assertSame(Long.TYPE, record.fields.get(0).type());
        Assertions.assertEquals("value", record.fields.get(0).name());
        Assertions.assertEquals(23, record.extractLongFrom(0));
        Assertions.assertEquals(23, record.extractLongFrom("value"));
    }

    @Test
    public void TestReadInt32Record() {
        var record = loadRecordWithValueColumn("Int32",4, new byte[]{23,0,0,0,0});

        Assertions.assertEquals(23, record.extractLongFrom(0));
    }

    private static YxdbRecord loadRecordWithValueColumn(String type, int size, byte[] sourceData) {
        var fields = new ArrayList<MetaInfoField>(1);
        fields.add(new MetaInfoField("value", type, size, 0));
        var record = YxdbRecord.newFromFieldList(fields);
        record.loadRecordBlobFrom(sourceData, 0, sourceData.length);
        return record;
    }
}

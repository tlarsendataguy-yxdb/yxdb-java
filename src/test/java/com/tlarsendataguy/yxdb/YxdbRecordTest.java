package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

public class YxdbRecordTest {
    @Test
    public void TestReadRecord() {
        var fields = new ArrayList<MetaInfoField>(1);
        fields.add(new MetaInfoField("value", "Int32", 4, 0));
        var record = YxdbRecord.generateFrom(fields);

        Assertions.assertEquals(1, record.fields.size());
        Assertions.assertSame(Long.TYPE, record.fields.get(0).type());
        Assertions.assertEquals("value", record.fields.get(0).name());

        var sourceData = new byte[]{23,0,0,0,0};
        record.loadFrom(sourceData, 0, 5);

        Assertions.assertEquals(23, record.extractLongFrom(0));
    }
}

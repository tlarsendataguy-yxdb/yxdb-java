package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class YxdbReaderTest {
    @Test
    public void TestGetReader() throws IOException, ParseException {
        var path = "src/test/resources/AllNormalFields.yxdb";
        var yxdb = YxdbReader.loadYxdb(path);
        Assertions.assertEquals(1, yxdb.numRecords);
        Assertions.assertNotNull(yxdb.metaInfoStr);
        Assertions.assertEquals(AllNormalFieldsMetaXml, yxdb.metaInfoStr);
        Assertions.assertEquals(16, yxdb.fields.size());

        int read = 0;
        while (yxdb.next()) {
            Assertions.assertEquals((byte)1, yxdb.readByte(0));
            Assertions.assertEquals((byte)1, yxdb.readByte("ByteField"));
            Assertions.assertTrue(yxdb.readBoolean(1));
            Assertions.assertTrue(yxdb.readBoolean("BoolField"));
            Assertions.assertEquals(16, yxdb.readLong(2));
            Assertions.assertEquals(16, yxdb.readLong("Int16Field"));
            Assertions.assertEquals(32, yxdb.readLong(3));
            Assertions.assertEquals(32, yxdb.readLong("Int32Field"));
            Assertions.assertEquals(64, yxdb.readLong(4));
            Assertions.assertEquals(64, yxdb.readLong("Int64Field"));
            Assertions.assertEquals(123.45, yxdb.readDouble(5));
            Assertions.assertEquals(123.45, yxdb.readDouble("FixedDecimalField"));
            Assertions.assertEquals(678.9f, yxdb.readDouble(6));
            Assertions.assertEquals(678.9f, yxdb.readDouble("FloatField"));
            Assertions.assertEquals(0.12345, yxdb.readDouble(7));
            Assertions.assertEquals(0.12345, yxdb.readDouble("DoubleField"));
            Assertions.assertEquals("A", yxdb.readString(8));
            Assertions.assertEquals("A", yxdb.readString("StringField"));
            Assertions.assertEquals("AB", yxdb.readString(9));
            Assertions.assertEquals("AB", yxdb.readString("WStringField"));
            Assertions.assertEquals("ABC", yxdb.readString(10));
            Assertions.assertEquals("ABC", yxdb.readString("V_StringShortField"));
            Assertions.assertEquals("B".repeat(500), yxdb.readString(11));
            Assertions.assertEquals("B".repeat(500), yxdb.readString("V_StringLongField"));
            Assertions.assertEquals("XZY", yxdb.readString(12));
            Assertions.assertEquals("XZY", yxdb.readString("V_WStringShortField"));
            Assertions.assertEquals("W".repeat(500), yxdb.readString(13));
            Assertions.assertEquals("W".repeat(500), yxdb.readString("V_WStringLongField"));

            var expectedDate = new SimpleDateFormat("yyyy-MM-dd").parse("2020-01-01");
            Assertions.assertEquals(expectedDate, yxdb.readDate(14));
            Assertions.assertEquals(expectedDate, yxdb.readDate("DateField"));

            var expectedDateTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2020-02-03 04:05:06");
            Assertions.assertEquals(expectedDateTime, yxdb.readDate(15));
            Assertions.assertEquals(expectedDateTime, yxdb.readDate("DateTimeField"));

            read++;
        }

        Assertions.assertEquals(1, read);
    }

    @Test
    public void TestLotsOfRecords() throws IOException {
        var path = "src/test/resources/LotsOfRecords.yxdb";
        var yxdb = YxdbReader.loadYxdb(path);

        long sum = 0;
        int index = 0;
        while (yxdb.next()) {
            var value = yxdb.readLong(0);
            if (index % 10000 == 0) {
                System.out.println("index " + index + ", value " + value);
            }
            sum += value;
            index++;
        }
        Assertions.assertEquals(5000050000L, sum);
    }

    String AllNormalFieldsMetaXml = """
<RecordInfo>
	<Field name="ByteField" source="TextInput:" type="Byte"/>
	<Field name="BoolField" source="Formula: 1" type="Bool"/>
	<Field name="Int16Field" source="Formula: 16" type="Int16"/>
	<Field name="Int32Field" source="Formula: 32" type="Int32"/>
	<Field name="Int64Field" source="Formula: 64" type="Int64"/>
	<Field name="FixedDecimalField" scale="6" size="19" source="Formula: 123.45" type="FixedDecimal"/>
	<Field name="FloatField" source="Formula: 678.9" type="Float"/>
	<Field name="DoubleField" source="Formula: 0.12345" type="Double"/>
	<Field name="StringField" size="64" source="Formula: &quot;A&quot;" type="String"/>
	<Field name="WStringField" size="64" source="Formula: &quot;AB&quot;" type="WString"/>
	<Field name="V_StringShortField" size="1000" source="Formula: &quot;ABC&quot;" type="V_String"/>
	<Field name="V_StringLongField" size="2147483647" source="Formula: PadLeft(&quot;&quot;, 500, &apos;B&apos;)" type="V_String"/>
	<Field name="V_WStringShortField" size="10" source="Formula: &quot;XZY&quot;" type="V_WString"/>
	<Field name="V_WStringLongField" size="1073741823" source="Formula: PadLeft(&quot;&quot;, 500, &apos;W&apos;)" type="V_WString"/>
	<Field name="DateField" source="Formula: &apos;2020-01-01&apos;" type="Date"/>
	<Field name="DateTimeField" source="Formula: &apos;2020-02-03 04:05:06&apos;" type="DateTime"/>
</RecordInfo>
""";
}

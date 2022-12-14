package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class YxdbReaderTest {
    @Test
    public void TestGetReader() throws IOException, ParseException {
        var path = "src/test/resources/AllNormalFields.yxdb";
        var yxdb = new YxdbReader(path);
        Assertions.assertEquals(1, yxdb.numRecords);
        Assertions.assertNotNull(yxdb.metaInfoStr);
        Assertions.assertEquals(AllNormalFieldsMetaXml, yxdb.metaInfoStr);
        Assertions.assertEquals(16, yxdb.listFields().size());

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
        var yxdb = new YxdbReader(path);

        long sum = 0;
        while (yxdb.next()) {
            sum += yxdb.readLong(0);
        }
        Assertions.assertEquals(5000050000L, sum);
    }

    @Test
    public void TestLoadReaderFromStream() throws IOException {
        var stream = new BufferedInputStream(new FileInputStream("src/test/resources/LotsOfRecords.yxdb"));
        var yxdb = new YxdbReader(stream);

        long sum = 0;
        while (yxdb.next()) {
            sum += yxdb.readLong(0);
        }
        Assertions.assertEquals(5000050000L, sum);
    }

    @Test
    public void RetrievingFieldWithWrongTypeThrows() throws IOException {
        var yxdb = new YxdbReader("src/test/resources/AllNormalFields.yxdb");
        yxdb.next();
        Assertions.assertThrows(IllegalArgumentException.class, ()->yxdb.readString(0));
        Assertions.assertThrows(IllegalArgumentException.class, ()->yxdb.readBoolean(0));
        Assertions.assertThrows(IllegalArgumentException.class, ()->yxdb.readBlob(0));
        Assertions.assertThrows(IllegalArgumentException.class, ()->yxdb.readDate(0));
        Assertions.assertThrows(IllegalArgumentException.class, ()->yxdb.readDouble(0));
        Assertions.assertThrows(IllegalArgumentException.class, ()->yxdb.readLong(0));
        Assertions.assertThrows(IllegalArgumentException.class, ()->yxdb.readByte(1));
        try {
            yxdb.readString(0);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void RetrievingFieldWithInvalidNameThrows() throws IOException {
        var yxdb = new YxdbReader("src/test/resources/AllNormalFields.yxdb");
        yxdb.next();
        Assertions.assertThrows(IllegalArgumentException.class, ()->yxdb.readByte("Invalid"));
        Assertions.assertThrows(IllegalArgumentException.class, ()->yxdb.readString("Invalid"));
        Assertions.assertThrows(IllegalArgumentException.class, ()->yxdb.readBoolean("Invalid"));
        Assertions.assertThrows(IllegalArgumentException.class, ()->yxdb.readBlob("Invalid"));
        Assertions.assertThrows(IllegalArgumentException.class, ()->yxdb.readDate("Invalid"));
        Assertions.assertThrows(IllegalArgumentException.class, ()->yxdb.readDouble("Invalid"));
        Assertions.assertThrows(IllegalArgumentException.class, ()->yxdb.readLong("Invalid"));
        try {
            yxdb.readString("Invalid");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void TestTutorialData() throws IOException {
        var yxdb = new YxdbReader("src/test/resources/TutorialData.yxdb");
        var mrCount = 0;
        while (yxdb.next()) {
            if (yxdb.readString("Prefix").equals("Mr")) {
                mrCount++;
            }
        }
        Assertions.assertEquals(4068, mrCount);
    }

    @Test
    public void TestNewYxdb() throws IOException {
        var yxdb = new YxdbReader("src/test/resources/TestNewYxdb.yxdb");
        byte sum = 0;
        while (yxdb.next()){
            sum += yxdb.readByte(1);
        }
        Assertions.assertEquals(6, sum);
    }

    @Test
    public void TestVeryLongField() throws IOException {
        var yxdb = new YxdbReader("src/test/resources/VeryLongField.yxdb");
        byte[] blob;

        yxdb.next();
        blob = yxdb.readBlob(1);
        Assertions.assertEquals(604732, blob.length);

        yxdb.next();
        blob = yxdb.readBlob("Blob");
        Assertions.assertNull(blob);

        yxdb.next();
        blob = yxdb.readBlob(1);
        Assertions.assertEquals(604732, blob.length);

        yxdb.close();
    }

    @Test
    public void TestInvalidFile() {
        try {
            new YxdbReader("src/test/resources/invalid.txt");
        } catch (Exception ex) {
            var msg = ex.getMessage();
            Assertions.assertEquals("file is not a valid YXDB file", msg);
            ex.printStackTrace();
        }
    }

    @Test
    public void TestSmallInvalidFile() {
        try {
            new YxdbReader("src/test/resources/invalidSmall.txt");
        } catch (Exception ex) {
            var msg = ex.getMessage();
            Assertions.assertEquals("file is not a valid YXDB file", msg);
            ex.printStackTrace();
        }
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

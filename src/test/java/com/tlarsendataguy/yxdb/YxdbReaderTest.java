package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

public class YxdbReaderTest {
    @Test
    public void TestGetReader() {
        var path = "src/test/resources/AllNormalFields.yxdb";
        try{
            var yxdb = YxdbReader.loadYxdb(path);
            Assertions.assertEquals(1, yxdb.numRecords);
            Assertions.assertEquals(1372, yxdb.metaInfoSize);
            Assertions.assertNotNull(yxdb.metaInfoStr);
            Assertions.assertEquals(AllNormalFieldsMetaXml, yxdb.metaInfoStr);
            Assertions.assertEquals(16, yxdb.fields.size());
        } catch (Exception ex){
            Assertions.fail(ex.toString());
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

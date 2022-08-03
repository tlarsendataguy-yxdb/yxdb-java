## yxdb-java

yxdb-java is a library for reading YXDB files into Java applications.

The library does not have external dependencies and is a pure Java solution.

The public API is contained in the YxdbReader class. Instantiate YxdbReader using the following methoods:
* `loadYxdb(String)` - load from a file
* `loadYxdb(InputStream)` - load from an in-memory stream

Iterate through the records in the file using the `next()` method in a while loop:

```
while (reader.next()) {
    // do something
}
```

Fields can be access via the `readX()` methods on the YxdbReader class. There are readers for each kind of data field supported by YXDB files:
* `readByte()` - read Byte fields
* `readBlob()` - read Blob and SpatialObj fields
* `readBoolean()` - read Bool fields
* `readDate()` - read Date and DateTime fields
* `readDouble()` - read FixedDecimal, Float, and Double fields
* `readLong()` - read Int16, Int32, and Int64 fields
* `readString()` - read String, WString, V_String, and V_WString fields

Each read method has 2 overloads:
* `getX(int index)` - read by field index number
* `getX(String name)` - read by field name

If either the index number or field name is invalid, the read methods will throw an `IllegalArgumentException`.
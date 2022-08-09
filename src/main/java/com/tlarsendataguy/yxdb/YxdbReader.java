package com.tlarsendataguy.yxdb;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.lang.Integer.parseInt;

/**
 * YxdbReader contains the public interface for reading .yxdb files.
 * <p>
 * There are 2 constructors available for YxdbReader. One constructor takes a file path string and another
 * takes an InputStream that reads yxdb-formatted bytes.
 */
public class YxdbReader {
    /**
     * Returns a reader that will parse the .yxdb file specified by the path argument.
     * <p>
     * Iterate through the records in the .yxdb file by calling next().
     * <p>
     * After each call to next(), access the data fields using the readX methods.
     * <p>
     * The reader's stream can be closed early by calling the close() method. If the file is read to the end (i.e. next() returns false), the stream is automatically closed.
     *
     * @param path                      the path to a .yxdb file
     * @throws IllegalArgumentException thrown when the provided file path does not exist or cannot be accessed
     * @throws IOException              thrown when there are issues reading the file
     */
    public YxdbReader(String path) throws IOException {
        this.path = path;
        var file = new File(this.path);
        stream = new BufferedInputStream(new FileInputStream(file));
        fields = new ArrayList<>();
        loadHeaderAndMetaInfo();
    }

    /**
     * Returns a reader that will parse the .yxdb file contained in the stream.
     * <p>
     * Iterate through the records in the .yxdb file by calling next().
     * <p>
     * After each call to next(), access the data fields using the readX methods.
     * <p>
     * The reader's stream can be closed early by calling the close() method. If the file is read to the end (i.e. next() returns false), the stream is automatically closed.
     *
     * @param stream       an InputStream for a .yxdb-formatted stream of bytes
     * @throws IOException thrown when there are issues reading the stream
     */
    public YxdbReader(BufferedInputStream stream) throws IOException {
        path = "";
        this.stream = stream;
        fields = new ArrayList<>();
        loadHeaderAndMetaInfo();
    }

    /**
     * The total number of records in the .yxdb file.
     */
    public long numRecords;
    private int metaInfoSize;
    /**
     * Contains the raw XML metadata from the .yxdb file.
     */
    public String metaInfoStr;
    private final List<MetaInfoField> fields;
    private final BufferedInputStream stream;
    private final String path;
    private YxdbRecord record;
    private BufferedRecordReader recordReader;

    /**
     * @return the list of fields in the .yxdb file. The index of each field in this list matches the index of the field in the .yxdb file.
     */
    public List<YxdbField> listFields() {
        return record.fields;
    }

    /**
     * Closes the stream manually if the reader needs to be ended before reaching the end of the file.
     *
     * @throws IOException thrown when the stream fails to close
     */
    public void close() throws IOException {
        stream.close();
    }

    /**
     * The next function is designed to iterate over each record in the .yxdb file.
     * <p>
     * The standard way of iterating through records is to use a while loop:
     * <p>
     * <code>
     * while (reader.next()) {
     *     // do something
     * }
     * </code>
     *
     * @return             true, if the next record was loaded, and false, if the end of the file was reached
     * @throws IOException thrown when there is an error reading the next record
     */
    public boolean next() throws IOException {
        return recordReader.nextRecord();
    }

    /**
     * Reads a byte field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the byte field at the specified index. May be null
     * @throws IllegalArgumentException thrown when the index is out of range or the field at the specified index is not a byte field
     */
    public Byte readByte(int index) throws IllegalArgumentException {
        return record.extractByteFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a byte field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified byte field. May be null.
     * @throws IllegalArgumentException thrown when the field does not exist or is not a byte field
     */
    public Byte readByte(String name) throws IllegalArgumentException {
        return record.extractByteFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a boolean field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the boolean field at the specified index. May be null.
     * @throws IllegalArgumentException thrown when the index is out of range or the field at the specified index is not a boolean field
     */
    public Boolean readBoolean(int index) throws IllegalArgumentException {
        return record.extractBooleanFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a boolean field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified boolean field. May be null.
     * @throws IllegalArgumentException thrown when the field does not exist or is not a boolean field
     */
    public Boolean readBoolean(String name) throws IllegalArgumentException {
        return record.extractBooleanFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a long integer field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the long integer field at the specified index. May be null.
     * @throws IllegalArgumentException thrown when the index is out of range or the field at the specified index is not a long integer field
     */
    public Long readLong(int index) throws IllegalArgumentException {
        return record.extractLongFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a long integer field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified long integer field. May be null.
     * @throws IllegalArgumentException thrown when the field does not exist or is not a long integer field
     */
    public Long readLong(String name) throws IllegalArgumentException {
        return record.extractLongFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a numeric field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the numeric field at the specified index. May be null.
     * @throws IllegalArgumentException thrown when the index is out of range or the field at the specified index is not a numeric field
     */
    public Double readDouble(int index) throws IllegalArgumentException {
        return record.extractDoubleFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a numeric field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified numeric field. May be null.
     * @throws IllegalArgumentException thrown when the field does not exist or is not a numeric field
     */
    public Double readDouble(String name) throws IllegalArgumentException {
        return record.extractDoubleFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a text field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the text field at the specified index. May be null.
     * @throws IllegalArgumentException thrown when the index is out of range or the field at the specified index is not a text field
     */
    public String readString(int index) throws IllegalArgumentException {
        return record.extractStringFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a text field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified text field. May be null.
     * @throws IllegalArgumentException thrown when the field does not exist or is not a text field
     */
    public String readString(String name) throws IllegalArgumentException {
        return record.extractStringFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a date/datetime field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the date/datetime field at the specified index. May be null.
     * @throws IllegalArgumentException thrown when the index is out of range or the field at the specified index is not a date/datetime field
     */
    public Date readDate(int index) throws IllegalArgumentException {
        return record.extractDateFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a date/datetime field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified date/datetime field. May be null.
     * @throws IllegalArgumentException thrown when the field does not exist or is not a date field
     */
    public Date readDate(String name) throws IllegalArgumentException {
        return record.extractDateFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a blob field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the blob field, as an array of bytes, at the specified index. May be null.
     * @throws IllegalArgumentException thrown when the index is out of range or the field at the specified index is not a blob field
     */
    public byte[] readBlob(int index) throws IllegalArgumentException {
        return record.extractBlobFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a blob field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified blob field, as an array of bytes. May be null.
     * @throws IllegalArgumentException thrown when the field does not exist or is not a blob field
     */
    public byte[] readBlob(String name) throws IllegalArgumentException {
        return record.extractBlobFrom(name, recordReader.recordBuffer);
    }

    private void loadHeaderAndMetaInfo() throws IOException, IllegalArgumentException {
        var header = getHeader();
        numRecords = header.getLong(104);
        metaInfoSize = header.getInt(80);
        loadMetaInfo();
        record = YxdbRecord.newFromFieldList(fields);
        recordReader = new BufferedRecordReader(stream, record.fixedSize, record.hasVar, numRecords);
    }

    private void loadMetaInfo() throws IOException, IllegalArgumentException {
        var metaInfoBytes = stream.readNBytes((metaInfoSize*2)-2); //YXDB strings are null-terminated, so exclude the last character
        if (metaInfoBytes.length < (metaInfoSize*2)-2) {
            closeStreamAndThrow();
        }
        var skipped = stream.skip(2);
        if (skipped != 2) {
            closeStreamAndThrow();
        }
        metaInfoStr = new String(metaInfoBytes, StandardCharsets.UTF_16LE);
        getFields();
    }

    private ByteBuffer getHeader() throws IOException, IllegalArgumentException {
        var headerBytes = new byte[512];
        var written = stream.readNBytes(headerBytes,0, 512);
        if (written < 512) {
            closeStreamAndThrow();
        }
        return ByteBuffer.wrap(headerBytes).order(ByteOrder.LITTLE_ENDIAN);
    }

    private void getFields() throws IllegalArgumentException {
        var nodes = getRecordInfoNodes();

        for (int i = 0; i < nodes.getLength(); i++) {
            var field = nodes.item(i);
            if (field.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }
            parseField(field);
        }
    }

    private NodeList getRecordInfoNodes() {
        Document doc;
        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            doc = builder.parse(new InputSource(new StringReader(metaInfoStr)));
            doc.getDocumentElement().normalize();
        } catch (Exception ex) {
            closeStreamAndThrow();
            throw new IllegalArgumentException();
        }

        var info = doc.getElementsByTagName("RecordInfo").item(0);
        return info.getChildNodes();
    }

    private void parseField(Node field) {
        var attributes = field.getAttributes();
        var name = attributes.getNamedItem("name");
        var size = attributes.getNamedItem("size");
        var type = attributes.getNamedItem("type");
        var scale = attributes.getNamedItem("scale");

        if (name == null || type == null) {
            closeStreamAndThrow();
            return;
        }

        var nameStr = name.getNodeValue();
        switch (type.getNodeValue()) {
            case "Byte" -> fields.add(new MetaInfoField(nameStr, "Byte", 1, 0));
            case "Bool" -> fields.add(new MetaInfoField(nameStr, "Bool", 1, 0));
            case "Int16" -> fields.add(new MetaInfoField(nameStr, "Int16", 2, 0));
            case "Int32" -> fields.add(new MetaInfoField(nameStr, "Int32", 4, 0));
            case "Int64" -> fields.add(new MetaInfoField(nameStr, "Int64", 8, 0));
            case "FixedDecimal" -> {
                if (scale == null || size == null) {
                    closeStreamAndThrow();
                    return;
                }
                fields.add(new MetaInfoField(nameStr, "FixedDecimal", parseInt(size.getNodeValue()), parseInt(scale.getNodeValue())));
            }
            case "Float" -> fields.add(new MetaInfoField(nameStr, "Float", 4, 0));
            case "Double" -> fields.add(new MetaInfoField(nameStr, "Double", 8, 0));
            case "String" -> {
                if (size == null) {
                    closeStreamAndThrow();
                    return;
                }
                fields.add(new MetaInfoField(nameStr, "String", parseInt(size.getNodeValue()), 0));
            }
            case "WString" -> {
                if (size == null) {
                    closeStreamAndThrow();
                    return;
                }
                fields.add(new MetaInfoField(nameStr, "WString", parseInt(size.getNodeValue()), 0));
            }
            case "V_String", "V_WString", "Blob", "SpatialObj" -> fields.add(new MetaInfoField(nameStr, type.getNodeValue(), 4, 0));
            case "Date" -> fields.add(new MetaInfoField(nameStr, "Date", 10, 0));
            case "DateTime" -> fields.add(new MetaInfoField(nameStr, "DateTime", 19, 0));
            default -> closeStreamAndThrow();
        }
    }

    private void closeStreamAndThrow() throws IllegalArgumentException {
        try {
            stream.close();
        } catch (Exception ex) {
            throw new IllegalArgumentException(String.format("file '%s' is an invalid yxdb file", path));
        }
    }
}

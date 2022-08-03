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


public class YxdbReader {
    private YxdbReader(String path) throws FileNotFoundException {
        this.path = path;
        var file = new File(this.path);
        stream = new FileInputStream(file);
        fields = new ArrayList<>();
    }

    private YxdbReader(InputStream stream) {
        path = "";
        this.stream = stream;
        fields = new ArrayList<>();
    }

    public long numRecords;
    private int metaInfoSize;
    public String metaInfoStr;
    private final List<MetaInfoField> fields;
    private final InputStream stream;
    private final String path;
    private YxdbRecord record;
    private BufferedRecordReader recordReader;

    /**
     * Returns a reader that will parse the .yxdb file specified by the path argument.
     * <p>
     * Iterate through the records in the .yxdb file by calling next().
     * <p>
     * After each call to next(), access the data fields using the readX methods.
     * <p>
     * The reader's stream can be closed early by calling the close() method. If the file is read to the end (i.e. next() returns false), the stream is automatically closed.
     *
     * @param path the path to a .yxdb file
     * @return     a reader for the specified .yxdb file. The file header and metadata will already have been read and parsed before this method returns.
     * @throws IllegalArgumentException thrown when the provided file path does not exist or cannot be accessed
     * @throws IOException              thrown when there are issues reading the stream
     */
    public static YxdbReader loadYxdb(String path) throws IllegalArgumentException, IOException {
        var reader = new YxdbReader(path);
        reader.loadHeaderAndMetaInfo();
        return reader;
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
     * @return             a reader for the specified .yxdb file. The file header and metadata will already have been read and parsed before this method returns.
     * @throws IOException thrown when there are issues reading the stream
     */
    public static YxdbReader loadYxdb(InputStream stream) throws IOException {
        var reader = new YxdbReader(stream);
        reader.loadHeaderAndMetaInfo();
        return reader;
    }

    /**
     * @return the list of fields in the .yxdb file
     */
    public List<YxdbField> listFields() {
        return record.fields;
    }

    /**
     * Closes the stream manually if the reader needs to be ended before reaching the end of the file.
     *
     * @throws IOException
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
     * @return true, if the next record was loaded, and false, if the end of the file was reached
     * @throws IOException
     */
    public boolean next() throws IOException {
        return recordReader.nextRecord();
    }

    /**
     * Reads a byte field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the byte field at the specified index. May be null
     */
    public Byte readByte(int index) {
        return record.extractByteFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a byte field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified byte field. May be null.
     */
    public Byte readByte(String name) {
        return record.extractByteFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a boolean field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the boolean field at the specified index. May be null.
     */
    public Boolean readBoolean(int index) {
        return record.extractBooleanFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a boolean field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified boolean field. May be null.
     */
    public Boolean readBoolean(String name) {
        return record.extractBooleanFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a long integer field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the long integer field at the specified index. May be null.
     */
    public Long readLong(int index) {
        return record.extractLongFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a long integer field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified long integer field. May be null.
     */
    public Long readLong(String name) {
        return record.extractLongFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a numeric field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the numeric field at the specified index. May be null.
     */
    public Double readDouble(int index) {
        return record.extractDoubleFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a numeric field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified numeric field. May be null.
     */
    public Double readDouble(String name) {
        return record.extractDoubleFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a text field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the text field at the specified index. May be null.
     */
    public String readString(int index) {
        return record.extractStringFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a text field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified text field. May be null.
     */
    public String readString(String name) {
        return record.extractStringFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a date/datetime field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the date/datetime field at the specified index. May be null.
     */
    public Date readDate(int index) {
        return record.extractDateFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a date/datetime field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified date/datetime field. May be null.
     */
    public Date readDate(String name) {
        return record.extractDateFrom(name, recordReader.recordBuffer);
    }

    /**
     * Reads a blob field from the .yxdb file
     * @param  index the index of the field to read, starting at 0
     * @return the value of the blob field, as an array of bytes, at the specified index. May be null.
     */
    public byte[] readBlob(int index) {
        return record.extractBlobFrom(index, recordReader.recordBuffer);
    }

    /**
     * Reads a blob field from the .yxdb file
     * @param  name the name of the field to read
     * @return the value of the specified blob field, as an array of bytes. May be null.
     */
    public byte[] readBlob(String name) {
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

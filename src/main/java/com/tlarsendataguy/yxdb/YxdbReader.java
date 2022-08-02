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
import java.util.List;

import static java.lang.Integer.parseInt;

public class YxdbReader {
    private YxdbReader(String path) throws FileNotFoundException {
        this.path = path;
        var file = new File(this.path);
        stream = new FileInputStream(file);
        fields = new ArrayList<>();
    }

    public long numRecords;
    public int metaInfoSize;
    public String metaInfoStr;
    public List<MetaInfoField> fields;
    private final FileInputStream stream;
    private final String path;
    private int currentRecord = 0;
    private YxdbRecord record;
    private ByteBuffer compressedBuffer = ByteBuffer.allocate(262144);
    private ByteBuffer uncompressedBuffer = ByteBuffer.allocate(262144);
    private int uncompressedSize;
    private int uncompressedIndex;
    private final ByteBuffer blockSizeBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    Lzf lzf;

    public static YxdbReader loadYxdb(String path) throws IllegalArgumentException, IOException {
        var reader = new YxdbReader(path);
        reader.loadHeaderAndMetaInfo();
        return reader;
    }

    public boolean next() throws IOException {
        if (currentRecord >= numRecords) {
            return false;
        }
        return loadNextRecord();
    }

    public Byte readByte(int index) {
        return record.extractByteFrom(index);
    }
    public Byte readByte(String name) {
        return record.extractByteFrom(name);
    }

    public Long readInt(int index) {
        return record.extractLongFrom(index);
    }
    public Long readInt(String name) {
        return record.extractLongFrom(name);
    }

    private void loadHeaderAndMetaInfo() throws IOException, IllegalArgumentException {
        var header = getHeader();
        numRecords = header.getLong(104);
        metaInfoSize = header.getInt(80);
        loadMetaInfo();
        record = YxdbRecord.newFromFieldList(fields);

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

    private boolean loadNextRecord() throws IOException {
        if (uncompressedIndex >= uncompressedSize) {
            var success = loadNewBlock();
            if (!success) {
                return false;
            }
        }

        var totalLength = calcTotalLength();
        record.loadRecordBlobFrom(uncompressedBuffer.array(), uncompressedIndex, uncompressedIndex+totalLength);
        uncompressedIndex += totalLength;
        currentRecord++;
        return currentRecord <= numRecords;
    }

    private int calcTotalLength() {
        if (record.hasVar) {
            return record.fixedSize + 4 + uncompressedBuffer.getInt(uncompressedIndex+record.fixedSize);
        } else {
            return record.fixedSize;
        }
    }

    private boolean loadNewBlock() throws IOException {
        uncompressedIndex = 0;
        blockSizeBuffer.position(0);
        var read = stream.readNBytes(blockSizeBuffer.array(), 0, 4);
        if (read < 4) {
            stream.close();
            return false;
        }
        var length = blockSizeBuffer.getInt();

        var checkBit = ((long)length&0xffffffffL) & 0x80000000;
        if (checkBit > 0) {
            System.out.println("loading uncompressed block after record " + currentRecord);
            var uncompressedLength = length & 0x7fffff;
            return loadUncompressedBlock(uncompressedLength);
        } else {
            System.out.println("loading compressed block after record " + currentRecord);
            return loadCompressedBlock(length);
        }
    }

    private boolean loadUncompressedBlock(int length) throws IOException {
        if (length < 0) {
            throw new IOException("cannot load an uncompressed block with length less than 0");
        }

        var uncompressedLength = length & 0x7fffff;
        if (uncompressedLength > compressedBuffer.capacity()) {
            allocateNewBuffers(uncompressedLength+100);
        }
        var read = stream.readNBytes(uncompressedBuffer.array(), 0, uncompressedLength);
        if (read < uncompressedLength) {
            stream.close();
            return false;
        }
        return true;
    }

    private boolean loadCompressedBlock(int length) throws IOException {
        if (length < 0) {
            throw new IOException("cannot load a compressed block with length less than 0");
        }

        if (length > compressedBuffer.capacity()) {
            allocateNewBuffers(length+100);
        }
        var read = stream.readNBytes(compressedBuffer.array(), 0, length);
        if (read != length) {
            stream.close();
            return false;
        }

        uncompressedSize = lzf.decompress(length);
        return true;
    }

    private void allocateNewBuffers(int compressedBufferSize) {
        int uncompressedBufferSize = 64000;
        if (compressedBufferSize > 22000) {
            uncompressedBufferSize = compressedBufferSize*3;
        }

        compressedBuffer = ByteBuffer.allocate(compressedBufferSize).order(ByteOrder.LITTLE_ENDIAN);
        uncompressedBuffer = ByteBuffer.allocate(uncompressedBufferSize).order(ByteOrder.LITTLE_ENDIAN);
        lzf = new Lzf(compressedBuffer.array(), uncompressedBuffer.array());
    }

    private void closeStreamAndThrow() throws IllegalArgumentException {
        try {
            stream.close();
        } catch (Exception ex) {
            throw new IllegalArgumentException(String.format("file '%s' is an invalid yxdb file", path));
        }
    }
}

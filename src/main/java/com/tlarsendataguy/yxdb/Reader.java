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

public class Reader {
    private Reader(String path) throws FileNotFoundException {
        this.path = path;
        var file = new File(this.path);
        stream = new FileInputStream(file);
        fields = new ArrayList<>();
    }

    public long numRecords;
    public int metaInfoSize;
    public String metaInfoStr;
    public List<Field> fields;
    private final FileInputStream stream;
    private final String path;

    public static Reader loadYxdb(String path) throws IllegalArgumentException, IOException {
        var reader = new Reader(path);
        reader.loadHeaderAndMetaInfo();
        return reader;
    }

    private void loadHeaderAndMetaInfo() throws IOException, IllegalArgumentException {
        var header = getHeader();
        numRecords = header.getLong(104);
        metaInfoSize = header.getInt(80);
        loadMetaInfo();
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
        var index = 0;

        for (int i = 0; i < nodes.getLength(); i++) {
            var field = nodes.item(i);
            if (field.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }
            parseField(field, index);
            index++;
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

    private void parseField(Node field, int index) {
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
            case "Byte" -> fields.add(new Field(index, nameStr, "Byte", 1, 0));
            case "Bool" -> fields.add(new Field(index, nameStr, "Bool", 1, 0));
            case "Int16" -> fields.add(new Field(index, nameStr, "Int16", 2, 0));
            case "Int32" -> fields.add(new Field(index, nameStr, "Int32", 4, 0));
            case "Int64" -> fields.add(new Field(index, nameStr, "Int64", 8, 0));
            case "FixedDecimal" -> {
                if (scale == null || size == null) {
                    closeStreamAndThrow();
                    return;
                }
                fields.add(new Field(index, nameStr, "FixedDecimal", parseInt(size.getNodeValue()), parseInt(scale.getNodeValue())));
            }
            case "Float" -> fields.add(new Field(index, nameStr, "Float", 4, 0));
            case "Double" -> fields.add(new Field(index, nameStr, "Double", 8, 0));
            case "String" -> {
                if (size == null) {
                    closeStreamAndThrow();
                    return;
                }
                fields.add(new Field(index, nameStr, "String", parseInt(size.getNodeValue()), 0));
            }
            case "WString" -> {
                if (size == null) {
                    closeStreamAndThrow();
                    return;
                }
                fields.add(new Field(index, nameStr, "WString", parseInt(size.getNodeValue()) * 2, 0));
            }
            case "V_String", "V_WString", "Blob", "SpatialObj" -> fields.add(new Field(index, nameStr, type.getNodeValue(), 4, 0));
            case "Date" -> fields.add(new Field(index, nameStr, "Date", 10, 0));
            case "DateTime" -> fields.add(new Field(index, nameStr, "DateTime", 19, 0));
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

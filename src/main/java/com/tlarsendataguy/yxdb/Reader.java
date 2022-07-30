package com.tlarsendataguy.yxdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class Reader {
    private Reader(String path) throws FileNotFoundException {
        this.path = path;
        var file = new File(this.path);
        stream = new FileInputStream(file);
    }

    public long numRecords;
    public int metaInfoSize;
    public String metaInfoStr;
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
        var metaInfoBytes = stream.readNBytes((metaInfoSize*2)-2); //YXDB strings are null-terminated, so exclude the last character
        if (metaInfoBytes.length < (metaInfoSize*2)-2) {
            closeStreamAndThrow();
        }
        var skipped = stream.skip(2);
        if (skipped != 2) {
            closeStreamAndThrow();
        }
        metaInfoStr = new String(metaInfoBytes, StandardCharsets.UTF_16LE);
    }
    private ByteBuffer getHeader() throws IOException {
        var headerBytes = new byte[512];
        var written = stream.readNBytes(headerBytes,0, 512);
        if (written < 512) {
            closeStreamAndThrow();
        }
        return ByteBuffer.wrap(headerBytes).order(ByteOrder.LITTLE_ENDIAN);
    }

    private void closeStreamAndThrow() throws IOException, IllegalArgumentException {
        stream.close();
        throw new IllegalArgumentException(String.format("file '%s' is an invalid yxdb file", path));
    }
}

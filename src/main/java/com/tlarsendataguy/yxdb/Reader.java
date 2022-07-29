package com.tlarsendataguy.yxdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Reader {
    private Reader(long numRecords, int metaInfoSize, String metaInfoStr) {
        this.numRecords = numRecords;
        this.metaInfoSize = metaInfoSize;
        this.metaInfoStr = metaInfoStr;
    }
    public long numRecords;
    public int metaInfoSize;
    public String metaInfoStr;

    public static Reader loadYxdb(String path) throws IllegalArgumentException, IOException {
        var file = new File(path);
        try (var stream = new FileInputStream(file)) {
            var header = getHeader(stream);
            var numRecords = header.getLong(104);
            var metaInfoSize = header.getInt(80);
            var metaInfoBytes = stream.readNBytes((metaInfoSize*2)-2); //YXDB strings are null-terminated, so exclude the last character
            var metaInfoStr = new String(metaInfoBytes, StandardCharsets.UTF_16LE);
            return new Reader(numRecords, metaInfoSize, metaInfoStr);
        }
    }

    private static ByteBuffer getHeader(FileInputStream stream) throws IOException {
        var headerBytes = new byte[512];
        var written = stream.readNBytes(headerBytes,0, 512);
        if (written < 512) {
            throw new IllegalArgumentException("file is an invalid yxdb file");
        }
        return ByteBuffer.wrap(headerBytes).order(ByteOrder.LITTLE_ENDIAN);
    }
}

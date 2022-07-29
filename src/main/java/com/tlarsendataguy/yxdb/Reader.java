package com.tlarsendataguy.yxdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Reader {
    private Reader(long numRecords) {
        this.numRecords = numRecords;
    }
    public long numRecords;

    public static Reader loadYxdb(String path) throws IllegalArgumentException, IOException {
        var file = new File(path);
        try (var stream = new FileInputStream(file)) {
            var header = getHeader(stream);
            var numRecords = header.getLong(104);
            return new Reader(numRecords);
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

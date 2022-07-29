package com.tlarsendataguy.yxdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;

public class Reader {
    private Reader(long numRecords) {
        this.numRecords = numRecords;
    }
    public long numRecords;

    public static Reader loadYxdb(String path) throws IllegalArgumentException, IOException {
        var file = new File(path);
        try (var stream = new FileInputStream(file)) {
            var header = getHeader(stream);
            var numRecords = extractLong(header, 104);
            return new Reader(numRecords);
        }
    }

    private static byte[] getHeader(FileInputStream stream) throws IOException {
        var header = new byte[512];
        var written = stream.readNBytes(header,0, 512);
        if (written < 512) {
            throw new IllegalArgumentException("file is an invalid yxdb file");
        }
        return header;
    }

    private static long extractLong(byte[] data, int startAt) {
        return java.nio.ByteBuffer.wrap(Arrays.copyOfRange(data, startAt,startAt+8)).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }
}

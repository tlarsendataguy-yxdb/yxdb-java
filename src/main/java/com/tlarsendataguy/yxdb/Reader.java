package com.tlarsendataguy.yxdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;

public class Reader {
    private Reader(int numRecords) {
        this.numRecords = numRecords;
    }
    public int numRecords;

    public static Reader loadYxdb(String path) throws FileNotFoundException, IOException {
        var file = new File(path);
        var stream = new FileInputStream(file);

        var header = new byte[512];
        var written = stream.read(header);
        if (written < 512) {
            throw new IllegalArgumentException(String.format("'%s' is an invalid yxdb file", path));
        }
        var numRecords = java.nio.ByteBuffer.wrap(Arrays.copyOfRange(header, 104,112)).order(ByteOrder.LITTLE_ENDIAN).getInt();
        return new Reader(numRecords);
    }
}

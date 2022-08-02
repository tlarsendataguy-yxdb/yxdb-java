package com.tlarsendataguy.yxdb;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class BufferedRecordReader {
    static int lzfBufferSize = 262144;
    public BufferedRecordReader(FileInputStream stream, int fixedLen, boolean hasVarFields) {
        this.stream = stream;
        this.fixedLen = fixedLen;
        this.hasVarFields = hasVarFields;
        if (hasVarFields) {
            recordBuffer = ByteBuffer.allocate(fixedLen + 4 + 1000);
        } else {
            recordBuffer = ByteBuffer.allocate(fixedLen);
        }
    }
    final FileInputStream stream;
    final int fixedLen;
    final boolean hasVarFields;

    final ByteBuffer lzfIn = ByteBuffer.allocate(lzfBufferSize).order(ByteOrder.LITTLE_ENDIAN);
    final ByteBuffer lzfOut = ByteBuffer.allocate(lzfBufferSize).order(ByteOrder.LITTLE_ENDIAN);
    ByteBuffer recordBuffer;

    public boolean nextRecord() {
        return true;
    }
}

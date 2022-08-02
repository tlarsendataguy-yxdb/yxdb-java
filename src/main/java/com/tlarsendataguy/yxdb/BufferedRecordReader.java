package com.tlarsendataguy.yxdb;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class BufferedRecordReader {
    static int lzfBufferSize = 262144;
    public BufferedRecordReader(FileInputStream stream, int fixedLen, boolean hasVarFields) {
        this.stream = stream;
        this.fixedLen = fixedLen;
        this.hasVarFields = hasVarFields;
        if (hasVarFields) {
            recordBuffer = ByteBuffer.allocate(fixedLen + 4 + 1000).order(ByteOrder.LITTLE_ENDIAN);
        } else {
            recordBuffer = ByteBuffer.allocate(fixedLen).order(ByteOrder.LITTLE_ENDIAN);
        }
        lzfIn = ByteBuffer.allocate(lzfBufferSize).order(ByteOrder.LITTLE_ENDIAN);
        lzfOut = ByteBuffer.allocate(lzfBufferSize).order(ByteOrder.LITTLE_ENDIAN);
        lzf = new Lzf(lzfIn.array(), lzfOut.array());
        lzfLengthBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    }
    final FileInputStream stream;
    final int fixedLen;
    final boolean hasVarFields;

    final ByteBuffer lzfIn;
    final ByteBuffer lzfOut;
    int lzfOutIndex;
    final Lzf lzf;
    final ByteBuffer lzfLengthBuffer;
    ByteBuffer recordBuffer;

    public boolean nextRecord() throws IOException {
        readUncompressedBytes(fixedLen);
        return true;
    }

    private void readUncompressedBytes(int size) throws IOException {
        while (size > 0) {
            var read = stream.readNBytes(lzfLengthBuffer.array(), 0, 4);
            var lzfBlockLength = lzfLengthBuffer.getInt(0);

            var checkbit = (long)lzfBlockLength & 0x80000000L;
            int readOut;
            if (checkbit > 0) {
                lzfBlockLength &= 0x7ffffff;
                readOut = stream.readNBytes(lzfOut.array(),0, lzfBlockLength);
            } else {
                stream.readNBytes(lzfIn.array(), 0, lzfBlockLength);
                readOut = lzf.decompress(lzfBlockLength);
            }
            var lenToCopy = Math.min(readOut, size);
            System.arraycopy(lzfOut.array(), 0, recordBuffer.array(), 0, lenToCopy);
            size -= lenToCopy;
        }
    }
}

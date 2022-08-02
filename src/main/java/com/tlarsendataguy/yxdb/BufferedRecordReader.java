package com.tlarsendataguy.yxdb;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class BufferedRecordReader {
    static int lzfBufferSize = 262144;
    public BufferedRecordReader(FileInputStream stream, int fixedLen, boolean hasVarFields, long totalRecords) {
        this.totalRecords = totalRecords;
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
    final long totalRecords;

    final ByteBuffer lzfIn;
    final ByteBuffer lzfOut;
    int lzfOutIndex;
    int lzfOutSize;
    final Lzf lzf;
    final ByteBuffer lzfLengthBuffer;
    ByteBuffer recordBuffer;
    int recordBufferIndex;
    long currentRecord;

    public boolean nextRecord() throws IOException {
        currentRecord++;
        if (currentRecord > totalRecords) {
            return false;
        }
        if (hasVarFields) {
            throw new IOException("variable length fields not supported yet");
        } else {
            return readFixedRecord();
        }
    }

    private boolean readFixedRecord() throws IOException {
        var size = fixedLen;
        recordBufferIndex = 0;

        while (size > 0) {
            if (lzfOutSize == 0) {
                System.out.println("load first lzf block");
                var read = stream.readNBytes(lzfLengthBuffer.array(), 0, 4);
                if (read < 4) {
                    return false;
                }
                var lzfBlockLength = lzfLengthBuffer.getInt(0);
                lzfOutSize = readLzfBlock(lzfBlockLength);
            }

            while (size + lzfOutIndex > lzfOutSize) {
                System.out.println("load next lzf block");
                var remainingLzf = lzfOutSize - lzfOutIndex;
                System.arraycopy(lzfOut.array(), lzfOutIndex, recordBuffer.array(), recordBufferIndex, remainingLzf);
                recordBufferIndex += remainingLzf;
                size -= remainingLzf;

                var read = stream.readNBytes(lzfLengthBuffer.array(), 0, 4);
                if (read < 4) {
                    return false;
                }
                var lzfBlockLength = lzfLengthBuffer.getInt(0);
                lzfOutSize = readLzfBlock(lzfBlockLength);
                lzfOutIndex = 0;
            }

            var lenToCopy = Math.min(lzfOutSize, size);
            System.arraycopy(lzfOut.array(), lzfOutIndex, recordBuffer.array(), recordBufferIndex, lenToCopy);
            lzfOutIndex += lenToCopy;
            recordBufferIndex += lenToCopy;
            size -= lenToCopy;
        }
        return true;
    }

    private int readLzfBlock(int lzfBlockLength) throws IOException{
        var checkbit = (long)lzfBlockLength & 0x80000000L;
        if (checkbit > 0) {
            lzfBlockLength &= 0x7ffffff;
            return stream.readNBytes(lzfOut.array(),0, lzfBlockLength);
        } else {
            var readIn = stream.readNBytes(lzfIn.array(), 0, lzfBlockLength);
            return lzf.decompress(readIn);
        }
    }
}

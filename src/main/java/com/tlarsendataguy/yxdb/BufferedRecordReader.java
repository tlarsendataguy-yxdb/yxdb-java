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
            stream.close();
            return false;
        }
        recordBufferIndex = 0;
        if (hasVarFields) {
            readVariableRecord();
        } else {
            read(fixedLen);
        }
        return true;
    }

    private void readVariableRecord() throws IOException {
        read(fixedLen+4);
        var varLength = recordBuffer.getInt(recordBufferIndex-4);
        if (fixedLen+4+varLength > recordBuffer.capacity()) {
            var newLength = (fixedLen+4+varLength) * 2;
            var newBuffer = ByteBuffer.allocate(newLength).order(ByteOrder.LITTLE_ENDIAN);
            System.arraycopy(recordBuffer.array(), 0, newBuffer.array(), 0, fixedLen+4);
            recordBuffer = newBuffer;
        }
        read(varLength);
    }

    private void read(int size) throws IOException {
        while (size > 0) {
            if (lzfOutSize == 0) {
                lzfOutSize = readNextLzfBlock();
            }

            while (size + lzfOutIndex > lzfOutSize) {
                size -= copyRemainingLzfOutToRecord();
                lzfOutSize = readNextLzfBlock();
                lzfOutIndex = 0;
            }

            var lenToCopy = Math.min(lzfOutSize, size);
            System.arraycopy(lzfOut.array(), lzfOutIndex, recordBuffer.array(), recordBufferIndex, lenToCopy);
            lzfOutIndex += lenToCopy;
            recordBufferIndex += lenToCopy;
            size -= lenToCopy;
        }
    }

    private int copyRemainingLzfOutToRecord() {
        var remainingLzf = lzfOutSize - lzfOutIndex;
        System.arraycopy(lzfOut.array(), lzfOutIndex, recordBuffer.array(), recordBufferIndex, remainingLzf);
        recordBufferIndex += remainingLzf;
        return remainingLzf;
    }

    private int readNextLzfBlock() throws IOException{
        var lzfBlockLength = readLzfBlockLength();
        var checkbit = (long)lzfBlockLength & 0x80000000L;
        if (checkbit > 0) {
            lzfBlockLength &= 0x7ffffff;
            return stream.readNBytes(lzfOut.array(),0, lzfBlockLength);
        } else {
            var readIn = stream.readNBytes(lzfIn.array(), 0, lzfBlockLength);
            return lzf.decompress(readIn);
        }
    }

    private int readLzfBlockLength() throws IOException {
        var read = stream.readNBytes(lzfLengthBuffer.array(), 0, 4);
        if (read < 4) {
            throw new IOException("yxdb file is not valid");
        }
        return lzfLengthBuffer.getInt(0);
    }
}

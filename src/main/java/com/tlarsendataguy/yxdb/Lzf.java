package com.tlarsendataguy.yxdb;

class Lzf {
    Lzf(byte[] inBuffer, byte[] outBuffer){
        this.inBuffer = inBuffer;
        this.outBuffer = outBuffer;
    }
    byte[] inBuffer;
    byte[] outBuffer;
    int inIndex;
    int outIndex;
    int inLen;

    public int decompress(int len) throws IllegalArgumentException {
        inLen = len;
        reset();

        if (inLen == 0) {
            return 0;
        }

        while (inIndex < inLen) {
            int ctrl = unsign(inBuffer[inIndex]);
            inIndex++;

            if (ctrl < 32) {
                copyByteSequence(ctrl);
            } else {
                expandRepeatedBytes(ctrl);
            }
        }

        return outIndex;
    }

    private void reset() {
        this.inIndex = 0;
        this.outIndex = 0;
    }

    private void copyByteSequence(int ctrl) throws IllegalArgumentException {
        int len = ctrl+1;
        if (outIndex + len > outBuffer.length) {
            throw new IllegalArgumentException("output array is too small");
        }
        System.arraycopy(inBuffer, inIndex, outBuffer, outIndex, len);
        outIndex += len;
        inIndex += len;
    }

    private void expandRepeatedBytes(int ctrl) throws IllegalArgumentException {
        int length = ctrl >> 5;
        int reference = outIndex - ((ctrl & 0x1f) << 8) - 1; // magic

        if (length == 7) { // when length is 7, the next byte has additional length
            length += unsign(inBuffer[inIndex]);
            inIndex++;
        }

        if (outIndex +length+2 > outBuffer.length) {
            throw new IllegalArgumentException("output array is too small");
        }

        reference -= unsign(inBuffer[inIndex]); // the next byte tells how far back the repeated bytes begin
        inIndex++;

        length += 2;

        while (length > 0) {
            var size = Math.min(length, outIndex - reference);
            reference = copyFromReferenceAndIncrement(reference, size);
            length -= size;
        }
    }

    private int copyFromReferenceAndIncrement(int reference, int size) {
        System.arraycopy(outBuffer, reference, outBuffer, outIndex, size);
        outIndex += size;
        return reference + size;
    }

    private static int unsign(byte value) {
        return value & 0xff; // Java's bytes are signed while the original algorithm is written for unsigned bytes
    }
}

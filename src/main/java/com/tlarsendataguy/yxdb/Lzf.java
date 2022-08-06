package com.tlarsendataguy.yxdb;

class Lzf {
    Lzf(byte[] inBuffer, byte[] outBuffer){
        this.inBuffer = inBuffer;
        this.outBuffer = outBuffer;
    }
    byte[] inBuffer;
    byte[] outBuffer;
    int iidx;
    int oidx;
    int inLen;

    public int decompress(int len) throws IllegalArgumentException {
        inLen = len;
        reset();

        if (inLen == 0) {
            return 0;
        }

        while (iidx < inLen) {
            int ctrl = unsign(inBuffer[iidx]);
            iidx++;

            if (ctrl < 32) {
                copyByteSequence(ctrl);
            } else {
                expandRepeatedBytes(ctrl);
            }
        }

        return oidx;
    }

    private void reset() {
        this.iidx = 0;
        this.oidx = 0;
    }

    private void copyByteSequence(int ctrl) throws IllegalArgumentException {
        int len = ctrl+1;
        if (oidx + len > outBuffer.length) {
            throw new IllegalArgumentException("output array is too small");
        }
        System.arraycopy(inBuffer, iidx, outBuffer, oidx, len);
        oidx += len;
        iidx += len;
    }

    private void expandRepeatedBytes(int ctrl) throws IllegalArgumentException {
        int length = ctrl >> 5;
        int reference = oidx - ((ctrl & 0x1f) << 8) - 1; // magic

        if (length == 7) { // when length is 7, the next byte has additional length
            length += unsign(inBuffer[iidx]);
            iidx++;
        }

        if (oidx+length+2 > outBuffer.length) {
            throw new IllegalArgumentException("output array is too small");
        }

        reference -= unsign(inBuffer[iidx]); // the next byte tells how far back the repeated bytes begin
        iidx++;

        length += 2;

        while (length > 0) {
            var size = Math.min(length, oidx - reference);
            reference = copyFromReferenceAndIncrement(reference, size);
            length -= size;
        }
    }

    private int copyFromReferenceAndIncrement(int reference, int size) {
        System.arraycopy(outBuffer, reference, outBuffer, oidx, size);
        oidx += size;
        return reference + size;
    }

    private static int unsign(byte value) {
        return value & 0xff; // Java's bytes are signed while the original algorithm is written for unsigned bytes
    }
}

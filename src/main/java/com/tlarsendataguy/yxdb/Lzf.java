package com.tlarsendataguy.yxdb;

class Lzf {
    Lzf(byte[] inData, byte[] outData){
        this.inData = inData;
        this.outData = outData;
    }
    byte[] inData;
    byte[] outData;
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
            int ctrl = unsign(inData[iidx]);
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
        if (oidx + len > outData.length) {
            throw new IllegalArgumentException("output array is too small");
        }
        System.arraycopy(inData, iidx, outData, oidx, len);
        oidx += len;
        iidx += len;
    }

    private void expandRepeatedBytes(int ctrl) throws IllegalArgumentException {
        int length = ctrl >> 5;
        int reference = oidx - ((ctrl & 0x1f) << 8) - 1; // magic

        if (length == 7) { // when length is 7, the next byte has additional length
            length += unsign(inData[iidx]);
            iidx++;
        }

        if (oidx+length+2 > outData.length) {
            throw new IllegalArgumentException("output array is too small");
        }

        reference -= unsign(inData[iidx]); // the next byte tells how far back the repeated bytes begin
        iidx++;

        reference = copyFromReferenceAndIncrement(reference);
        reference = copyFromReferenceAndIncrement(reference);

        while (length > 0) {
            reference = copyFromReferenceAndIncrement(reference);
            length--;
        }
    }

    private int copyFromReferenceAndIncrement(int reference) {
        outData[oidx] = outData[reference];
        oidx++;
        return reference+1;
    }

    private static int unsign(byte value) {
        return value & 0xff; // Java's bytes are signed while the original algorithm is written for unsigned bytes
    }
}

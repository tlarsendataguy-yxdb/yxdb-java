package com.tlarsendataguy.yxdb;

class Lzf {
    private Lzf(Byte[] inData, Byte[] outData){
        this.inData = inData;
        this.outData = outData;
        this.iidx = 0;
        this.oidx = 0;
        this.inLen = inData.length;
    }
    Byte[] inData;
    Byte[] outData;
    int iidx;
    int oidx;
    int inLen;

    private int execute() {
        if (inLen == 0) {
            return 0;
        }

        while (iidx < inLen) {
            Byte ctrl = inData[iidx];
            iidx++;

            if (ctrl < 32) {
                copyUncompressedBlock(ctrl);
            } else {
                throw new IllegalArgumentException();
            }
        }

        return oidx;
    }

    private void copyUncompressedBlock(Byte ctrl) throws IllegalArgumentException {
        int len = ctrl+1;
        if (oidx + len > outData.length) {
            throw new IllegalArgumentException("output array is too small");
        }
        System.arraycopy(inData, iidx, outData, oidx, len);
        oidx += len;
        iidx += len;
    }

    public static int decompress(Byte[] inData, Byte[] outData) throws IllegalArgumentException {
        Lzf lzf = new Lzf(inData, outData);
        return lzf.execute();
    }
}

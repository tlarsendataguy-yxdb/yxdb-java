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

        Byte ctrl = inData[iidx];
        iidx++;

        if (ctrl >= 32) {
            throw new IllegalArgumentException();
        }

        boolean doInnerLoop = true;
        while (doInnerLoop) {
            outData[oidx] = inData[iidx];
            oidx++;
            iidx++;

            ctrl--;
            doInnerLoop = ctrl > 0;
        }

        return oidx;
    }

    public static int decompress(Byte[] inData, Byte[] outData) throws IllegalArgumentException {
        Lzf lzf = new Lzf(inData, outData);
        return lzf.execute();
    }
}

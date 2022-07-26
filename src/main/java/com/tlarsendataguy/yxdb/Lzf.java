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

        if (ctrl < 32) {
            handleSmallControlValue(ctrl);
        } else {
            throw new IllegalArgumentException();
        }

        return oidx;
    }

    private void handleSmallControlValue(Byte ctrl) throws IllegalArgumentException {
        ctrl++;
        if (oidx + ctrl > outData.length) {
            throw new IllegalArgumentException("output array is too small");
        }
        while (ctrl > 0) {
            outData[oidx] = inData[iidx];
            oidx++;
            iidx++;

            ctrl--;
        }
    }

    public static int decompress(Byte[] inData, Byte[] outData) throws IllegalArgumentException {
        Lzf lzf = new Lzf(inData, outData);
        return lzf.execute();
    }
}

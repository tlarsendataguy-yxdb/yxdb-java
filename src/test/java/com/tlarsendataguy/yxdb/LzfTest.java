package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

public class LzfTest {
    @Test
    public void TestEmptyInput() {
        byte[] in = new byte[]{};
        byte[] out = new byte[]{};
        Lzf lzf = new Lzf(in, out);

        int written = lzf.decompress(0);
        Assertions.assertEquals(0, written);
    }

    @Test
    public void SmallControlValuesDoSimpleCopies() {
        byte[] in = new byte[]{0, 25};
        byte[] out = new byte[]{0};
        Lzf lzf = new Lzf(in, out);

        int written = lzf.decompress(2);
        Assertions.assertEquals(1, written);
        Assertions.assertEquals(25, out[0]);
    }

    @Test
    public void OutputArrayIsTooSmall() {
        byte[] in = new byte[]{0, 25};
        byte[] out = new byte[]{};
        Lzf lzf = new Lzf(in, out);

        Assertions.assertThrows(IllegalArgumentException.class, ()->lzf.decompress(2));
    }

    @Test
    public void LargerSmallControlValue() {
        byte[] in = new byte[]{4, 1, 2, 3, 4, 5};
        byte[] out = new byte[5];
        Lzf lzf = new Lzf(in, out);

        int written = lzf.decompress(6);
        Assertions.assertEquals(5, written);
        Assertions.assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, out);
    }

    @Test
    public void MultipleSmallControlValues() {
        byte[] in = new byte[]{2, 1, 2, 3, 1, 1, 2};
        byte[] out = new byte[5];
        Lzf lzf = new Lzf(in, out);

        int written = lzf.decompress(7);
        Assertions.assertEquals(5, written);
        Assertions.assertArrayEquals(new byte[]{1, 2, 3, 1, 2}, out);
    }

    @Test
    public void ExpandLargeControlValues() {
        byte[] in = new byte[]{2, 1, 2, 3, 32, 1};
        byte[] out = new byte[6];
        Lzf lzf = new Lzf(in, out);

        int written = lzf.decompress(6);
        Assertions.assertEquals(6, written);
        Assertions.assertArrayEquals(new byte[]{1, 2, 3, 2, 3, 2}, out);
    }

    @Test
    public void LargeControlValuesWithLengthOf7() {
        byte[] in = new byte[]{8, 1, 2, 3, 4, 5, 6, 7, 8, 9, (byte)224, 1, 8};
        byte[] out = new byte[19];
        Lzf lzf = new Lzf(in, out);

        int written = lzf.decompress(13);
        Assertions.assertEquals(19, written);
        Assertions.assertArrayEquals(new byte[]{1,2,3,4,5,6,7,8,9,1,2,3,4,5,6,7,8,9,1}, out);
    }

    @Test
    public void OutputArrayTooSmallForLargeControlValue() {
        byte[] in = new byte[]{8, 1, 2, 3, 4, 5, 6, 7, 8, 9, (byte)224, 1, 8};
        byte[] out = new byte[17];
        Lzf lzf = new Lzf(in, out);

        Assertions.assertThrows(IllegalArgumentException.class, ()->lzf.decompress(13));
    }

    @Test
    public void ResetLzfAndStartAgain() {
        byte[] in = new byte[]{4, 1, 2, 3, 4, 5};
        byte[] out = new byte[5];
        Lzf lzf = new Lzf(in, out);

        lzf.decompress(6);

        in[0] = 2;
        in[1] = 6;
        in[2] = 7;
        in[3] = 8;

        int written = lzf.decompress(4);
        Assertions.assertEquals(3, written);
        Assertions.assertArrayEquals(new byte[]{6, 7, 8, 4, 5}, out);
    }

    @Test
    public void Sandbox(){
        System.out.println((byte)224);
        System.out.println((byte)224 >> 5);
        System.out.println(-32 >> 5);
        System.out.println((224 & 0x1f) << 8);
        System.out.println((-32 & 0x1f) << 8);
        System.out.println((byte)(7 << 5));
        System.out.println(-32 & 0xff);
    }
}

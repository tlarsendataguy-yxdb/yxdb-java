package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

public class LzfTest {
    @Test
    public void TestEmptyInput() {
        byte[] in = new byte[]{};
        byte[] out = new byte[]{};

        int written = Lzf.decompress(in, out);
        Assertions.assertEquals(0, written);
    }

    @Test
    public void SmallControlValuesDoSimpleCopies() {
        byte[] in = new byte[]{0, 25};
        byte[] out = new byte[]{0};

        int written = Lzf.decompress(in, out);
        Assertions.assertEquals(1, written);
        Assertions.assertEquals(25, out[0]);
    }

    @Test
    public void OutputArrayIsTooSmall() {
        byte[] in = new byte[]{0, 25};
        byte[] out = new byte[]{};

        Assertions.assertThrows(IllegalArgumentException.class, ()->Lzf.decompress(in, out));
    }

    @Test
    public void LargerSmallControlValue() {
        byte[] in = new byte[]{4, 25, 30, 1, 22, 99};
        byte[] out = new byte[5];

        int written = Lzf.decompress(in, out);
        Assertions.assertEquals(5, written);
        Assertions.assertArrayEquals(new byte[]{25, 30, 1, 22, 99}, out);
    }

    @Test
    public void MultipleSmallControlValues() {
        byte[] in = new byte[]{2, 25, 30, 1, 1, 99, 22};
        byte[] out = new byte[5];

        int written = Lzf.decompress(in, out);
        Assertions.assertEquals(5, written);
        Assertions.assertArrayEquals(new byte[]{25, 30, 1, 99, 22}, out);
    }

    @Test
    public void ExpandLargeControlValues() {
        byte[] in = new byte[]{2, 25, 30, 1, 32, 2};
        byte[] out = new byte[6];

        int written = Lzf.decompress(in, out);
        Assertions.assertEquals(6, written);
        Assertions.assertArrayEquals(new byte[]{25, 30, 1, 25, 30, 1}, out);
    }

    @Test
    public void LargeControlValuesWithLengthOf7() {
        byte[] in = new byte[]{9, 1, 2, 3, 4, 5, 6, 7, 8, 9, (byte)224, 2};
        byte[] out = new byte[6];

        //int written = Lzf.decompress(in, out);
        //Assertions.assertEquals(6, written);
        //Assertions.assertArrayEquals(new byte[]{25, 30, 1, 25, 30, 1}, out);
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

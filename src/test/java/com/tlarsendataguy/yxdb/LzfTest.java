package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

public class LzfTest {
    @Test
    public void TestEmptyInput() {
        Byte[] in = new Byte[]{};
        Byte[] out = new Byte[]{};

        int written = Lzf.decompress(in, out);
        Assertions.assertEquals(0, written);
    }

    @Test
    public void SmallControlValuesDoSimpleCopies() {
        Byte[] in = new Byte[]{0, 25};
        Byte[] out = new Byte[]{0};

        int written = Lzf.decompress(in, out);
        Assertions.assertEquals(1, written);
        Assertions.assertEquals(25, (int)out[0]);
    }

    @Test
    public void OutputArrayIsTooSmall() {
        Byte[] in = new Byte[]{0, 25};
        Byte[] out = new Byte[]{};

        Assertions.assertThrows(IllegalArgumentException.class, ()->Lzf.decompress(in, out));
    }

    @Test
    public void LargerSmallControlValue() {
        Byte[] in = new Byte[]{4, 25, 30, 1, 22, 99};
        Byte[] out = new Byte[5];

        int written = Lzf.decompress(in, out);
        Assertions.assertEquals(5, written);
        Assertions.assertArrayEquals(new Byte[]{25, 30, 1, 22, 99}, out);
    }

    @Test
    public void Sandbox(){
        System.out.println(1<<5);
        System.out.println(32>>5);
        System.out.println((32 & 0x1f) << 8);
    }
}

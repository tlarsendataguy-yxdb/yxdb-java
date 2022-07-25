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
        Byte[] in = new Byte[]{1, 25};
        Byte[] out = new Byte[]{0};

        int written = Lzf.decompress(in, out);
        Assertions.assertEquals(1, written);
        Assertions.assertEquals(25, (int)out[0]);
    }
}

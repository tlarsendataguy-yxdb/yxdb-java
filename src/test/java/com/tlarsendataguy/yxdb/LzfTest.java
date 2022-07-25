package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

public class LzfTest {
    @Test
    public void TestEmptyInput() {
        Byte[] in = new Byte[]{};
        Byte[] out = new Byte[]{};

        int written = lzf.decompress(in, out);
        Assertions.assertEquals(0, written);
    }
}

package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ExtractorsTest {
    @Test
    public void Int16ExtractorAtBeginning(){
        var extract = Extractors.NewInt16Extractor(0);

        var buffer = ByteBuffer.wrap(new byte[]{10, 0}).order(ByteOrder.LITTLE_ENDIAN);
        var result = extract.apply(buffer);

        Assertions.assertEquals(10, result);
    }
}

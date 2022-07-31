package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;

public class ExtractorsTest {
    @Test
    public void ExtractInt16(){
        var extract = Extractors.NewInt16Extractor(2);
        Long result = extractFromBuffer(extract, new byte[]{0, 0, 10,0,0,0});

        Assertions.assertEquals(10, result);
    }

    @Test
    public void ExtractNullInt16(){
        var extract = Extractors.NewInt16Extractor(2);
        Long result = extractFromBuffer(extract, new byte[]{0, 0, 10,0,1,0});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractInt32(){
        var extract = Extractors.NewInt32Extractor(3);
        Long result = extractFromBuffer(extract, new byte[]{0, 0, 0, 10,0,0,0, 0});

        Assertions.assertEquals(10, result);
    }

    @Test
    public void ExtractNullInt32(){
        var extract = Extractors.NewInt32Extractor(3);
        Long result = extractFromBuffer(extract, new byte[]{0, 0, 0, 10,0,0,0, 1});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractInt64(){
        var extract = Extractors.NewInt64Extractor(4);
        Long result = extractFromBuffer(extract, new byte[]{0,0,0,0,10,0,0,0,0,0,0,0, 0});

        Assertions.assertEquals(10, result);
    }

    @Test
    public void ExtractNullInt64(){
        var extract = Extractors.NewInt64Extractor(4);
        Long result = extractFromBuffer(extract, new byte[]{0,0,0,0,10,0,0,0,0,0,0,0, 1});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractBool(){
        var extract = Extractors.NewBoolExtractor(4);
        Boolean result = extractFromBuffer(extract, new byte[]{0,0,0,0,1,0,0,0,0,0,0,0, 0});
        Assertions.assertTrue(result);

        result = extractFromBuffer(extract, new byte[]{0,0,0,0,0,0,0,0,0,0,0,0, 0});
        Assertions.assertFalse(result);
    }

    @Test
    public void ExtractNullBool(){
        var extract = Extractors.NewBoolExtractor(4);
        Boolean result = extractFromBuffer(extract, new byte[]{0,0,0,0,2,0,0,0,0,0,0,0, 1});

        Assertions.assertNull(result);
    }

    private static <T> T extractFromBuffer(Function<ByteBuffer, T> extract, byte[] data){
        var buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        return extract.apply(buffer);
    }
}

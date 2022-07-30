package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;

public class ExtractorsTest {
    @Test
    public void Int16ExtractorAtBeginning(){
        var extract = Extractors.NewInt16Extractor(0);
        Long result = extractFromBuffer(extract, new byte[]{10,0,0,0});

        Assertions.assertEquals(10, result);
    }

    @Test
    public void Int16ExtractorInMiddle(){
        var extract = Extractors.NewInt16Extractor(2);
        Long result = extractFromBuffer(extract, new byte[]{0, 0, 10,0,0,0});

        Assertions.assertEquals(10, result);
    }

    @Test
    public void Int16ExtractNull(){
        var extract = Extractors.NewInt16Extractor(2);
        Long result = extractFromBuffer(extract, new byte[]{0, 0, 10,0,1,0});

        Assertions.assertNull(result);
    }

    @Test
    public void Int32ExtractorAtBeginning(){
        var extract = Extractors.NewInt32Extractor(0);
        Long result = extractFromBuffer(extract, new byte[]{10,0,0,0, 0});

        Assertions.assertEquals(10, result);
    }

    @Test
    public void Int32ExtractorInMiddle(){
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

    private static <T> T extractFromBuffer(Function<ByteBuffer, T> extract, byte[] data){
        var buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        return extract.apply(buffer);
    }
}

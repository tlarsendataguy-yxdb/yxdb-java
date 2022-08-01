package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
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

    @Test
    public void ExtractByte(){
        var extract = Extractors.NewByteExtractor(4);
        Byte result = extractFromBuffer(extract, new byte[]{0,0,0,0,10,0,0,0,0,0,0,0, 0});

        Assertions.assertEquals((byte)10, result);
    }

    @Test
    public void ExtractNullByte(){
        var extract = Extractors.NewByteExtractor(4);
        Byte result = extractFromBuffer(extract, new byte[]{0,0,0,0,2,1,0,0,0,0,0,0, 1});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractFloat() {
        var extract = Extractors.NewFloatExtractor(4);
        Double result = extractFromBuffer(extract, new byte[]{0,0,0,0,-51,-52,-116,63,0,0,0,0, 0});

        Assertions.assertEquals(1.1f, result);
    }

    @Test
    public void ExtractNullFloat() {
        var extract = Extractors.NewFloatExtractor(4);
        Double result = extractFromBuffer(extract, new byte[]{0,0,0,0,-51,-52,-116,63,1,0,0,0, 0});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractDouble() {
        var extract = Extractors.NewDoubleExtractor(4);
        Double result = extractFromBuffer(extract, new byte[]{0,0,0,0,-102,-103,-103,-103,-103,-103,-15,63,0});

        Assertions.assertEquals(1.1, result);
    }

    @Test
    public void ExtractNullDouble() {
        var extract = Extractors.NewDoubleExtractor(4);
        Double result = extractFromBuffer(extract, new byte[]{0,0,0,0,-102,-103,-103,-103,-103,-103,-15,63,1});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractDate() throws ParseException {
        var extract = Extractors.NewDateExtractor(4);
        Date result = extractFromBuffer(extract, new byte[]{0,0,0,0,50,48,50,49,45,48,49,45,48,49,0});

        Assertions.assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-01"), result);
    }

    @Test
    public void ExtractNullDate() {
        var extract = Extractors.NewDateExtractor(4);
        Date result = extractFromBuffer(extract, new byte[]{0,0,0,0,50,48,50,49,45,48,49,45,48,49,1});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractDateTime() throws ParseException {
        var extract = Extractors.NewDateTimeExtractor(4);
        Date result = extractFromBuffer(extract, new byte[]{0,0,0,0,50,48,50,49,45,48,49,45,48,50,32,48,51,58,48,52,58,48,53,0});

        Assertions.assertEquals(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2021-01-02 03:04:05"), result);
    }

    @Test
    public void ExtractNullDateTime() {
        var extract = Extractors.NewDateTimeExtractor(4);
        Date result = extractFromBuffer(extract, new byte[]{0,0,0,0,50,48,50,49,45,48,49,45,48,50,32,48,51,58,48,52,58,48,53,1});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractString() {
        var extract = Extractors.NewStringExtractor(2, 15);
        String result = extractFromBuffer(extract, new byte[]{0, 0, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33, 0, 23, 77, 0});

        Assertions.assertEquals("hello world!", result);
    }

    @Test
    public void ExtractFullString() {
        var extract = Extractors.NewStringExtractor(2, 5);
        String result = extractFromBuffer(extract, new byte[]{0, 0, 104, 101, 108, 108, 111, 0});

        Assertions.assertEquals("hello", result);
    }

    @Test
    public void ExtractNullString() {
        var extract = Extractors.NewStringExtractor(2, 5);
        String result = extractFromBuffer(extract, new byte[]{0, 0, 104, 101, 108, 108, 111, 1});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractFixedDecimal() {
        var extract = Extractors.NewFixedDecimalExtractor(2, 10);
        Double result = extractFromBuffer(extract, new byte[]{0, 0, 49, 50, 51, 46, 52, 53, 0, 43, 67, 110, 0});

        Assertions.assertEquals(123.45, result);
    }

    @Test
    public void ExtractNullFixedDecimal() {
        var extract = Extractors.NewFixedDecimalExtractor(2, 10);
        Double result = extractFromBuffer(extract, new byte[]{0, 0, 49, 50, 51, 46, 52, 53, 0, 43, 67, 110, 1});

        Assertions.assertNull(result);
    }

    @Test
    public void ExtractWString() {
        var extract = Extractors.NewWStringExtractor(2, 15);
        String result = extractFromBuffer(extract, new byte[]{0, 0, 104, 0, 101, 0, 108, 0, 108, 0, 111, 0, 32, 0, 119, 0, 111, 0, 114, 0, 108, 0, 100, 0, 0, 0, 12, 0, 44, 0, 55, 0, 0});

        Assertions.assertEquals("hello world", result);
    }

    @Test
    public void ExtractNullWString() {
        var extract = Extractors.NewWStringExtractor(2, 15);
        String result = extractFromBuffer(extract, new byte[]{0, 0, 104, 0, 101, 0, 108, 0, 108, 0, 111, 0, 32, 0, 119, 0, 111, 0, 114, 0, 108, 0, 100, 0, 0, 0, 12, 0, 44, 0, 55, 0, 1});

        Assertions.assertNull(result);
    }

    @Test
    public void Sandbox() {
        var value = "hello world";

        System.out.println(Arrays.toString(value.getBytes(StandardCharsets.UTF_16LE)));

    }

    private static <T> T extractFromBuffer(Function<ByteBuffer, T> extract, byte[] data){
        var buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        return extract.apply(buffer);
    }
}

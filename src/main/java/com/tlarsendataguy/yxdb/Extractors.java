package com.tlarsendataguy.yxdb;


import java.nio.ByteBuffer;
import java.util.function.Function;

public class Extractors {
    public static Function<ByteBuffer, Long> NewInt16Extractor(int start) {
        return (buffer) -> {
            var value = buffer.getShort(start);
            return (long)value;
        };
    }

    public static Function<ByteBuffer, Long> NewInt32Extractor(int start) {
        return (buffer) -> {
            var value = buffer.getInt(start);
            return (long)value;
        };
    }
}

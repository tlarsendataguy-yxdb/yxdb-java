package com.tlarsendataguy.yxdb;


import java.nio.ByteBuffer;
import java.util.function.Function;

public class Extractors {
    public static Function<ByteBuffer, Long> NewInt16Extractor(int start) {
        return (buffer) -> {
            if (buffer.get(start+2) == 1) {
                return null;
            }
            var value = buffer.getShort(start);
            return (long)value;
        };
    }

    public static Function<ByteBuffer, Long> NewInt32Extractor(int start) {
        return (buffer) -> {
            if (buffer.get(start+4) == 1) {
                return null;
            }
            var value = buffer.getInt(start);
            return (long)value;
        };
    }

    public static Function<ByteBuffer, Long> NewInt64Extractor(int start) {
        return (buffer) -> {
            if (buffer.get(start+8) == 1) {
                return null;
            }
            return buffer.getLong(start);
        };
    }
}

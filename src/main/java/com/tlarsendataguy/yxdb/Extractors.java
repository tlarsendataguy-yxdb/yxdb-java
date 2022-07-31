package com.tlarsendataguy.yxdb;


import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Function;

public class Extractors {
    public static Function<ByteBuffer, Boolean> NewBoolExtractor(int start) {
        return (buffer) -> {
            var value = buffer.get(start);
            if (value == 2) {
                return null;
            }
            return buffer.get(start) == 1;
        };
    }

    public static Function<ByteBuffer, Byte> NewByteExtractor(int start) {
        return (buffer) -> {
            if (buffer.get(start+1) == 1) {
                return null;
            }
            return buffer.get(start);
        };
    }

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

    public static Function<ByteBuffer, Double> NewFloatExtractor(int start) {
        return (buffer) -> {
            if (buffer.get(start+4) == 1) {
                return null;
            }
            return (double)buffer.getFloat(start);
        };
    }

    public static Function<ByteBuffer, Double> NewDoubleExtractor(int start) {
        return (buffer) -> {
            if (buffer.get(start+8) == 1) {
                return null;
            }
            return (double)buffer.getDouble(start);
        };
    }

    private static final DateFormat date = new SimpleDateFormat("yyyy-MM-dd");
    public static Function<ByteBuffer, Date> NewDateExtractor(int start) {
        return (buffer) -> {
            if (buffer.get(start+10) == 1) {
                return null;
            }
            var str = new String(buffer.array(), start, 10, StandardCharsets.UTF_8);
            try {
                return date.parse(str);
            } catch (ParseException ex) {
                return null;
            }
        };
    }
}

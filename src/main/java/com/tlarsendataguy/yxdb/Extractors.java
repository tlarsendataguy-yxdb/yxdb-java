package com.tlarsendataguy.yxdb;


import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.function.Function;

public class Extractors {
    private static final DateFormat date = new SimpleDateFormat("yyyy-MM-dd");
    private static final DateFormat dateTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

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

    public static Function<ByteBuffer, Date> NewDateExtractor(int start) {
        return (buffer) -> {
            if (buffer.get(start+10) == 1) {
                return null;
            }
            return parseDate(buffer, start, 10, date);
        };
    }

    public static Function<ByteBuffer, Date> NewDateTimeExtractor(int start) {
        return (buffer) -> {
            if (buffer.get(start+19) == 1) {
                return null;
            }
            return parseDate(buffer, start, 19, dateTime);
        };
    }

    public static Function<ByteBuffer, String> NewStringExtractor(int start, int length) {
        return (buffer) -> {
            int end = getEndOfStringPos(buffer, start, length);
            return new String(Arrays.copyOfRange(buffer.array(), start, end), StandardCharsets.UTF_8);
        };
    }

    private static Date parseDate(ByteBuffer buffer, int start, int length, DateFormat format) {
        var str = new String(buffer.array(), start, length, StandardCharsets.UTF_8);
        try {
            return format.parse(str);
        } catch (ParseException ex) {
            return null;
        }
    }

    private static int getEndOfStringPos(ByteBuffer buffer, int start, int fieldLength) {
        int fieldTo = start + fieldLength;
        int strLen = 0;
        for (var i = start; i < fieldTo; i++) {
            if (buffer.get(i) == 0) {
                break;
            }
            strLen++;
        }
        return start+strLen;
    }
}

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

    public static Function<ByteBuffer, Double> NewFixedDecimalExtractor(int start, int fieldLength) {
        return (buffer) -> {
            if (buffer.get(start + fieldLength) == 1){
                return null;
            }
            var str = getString(buffer, start, fieldLength, 1);
            return Double.parseDouble(str);
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

    public static Function<ByteBuffer, String> NewStringExtractor(int start, int fieldLength) {
        return (buffer) -> {
            if (buffer.get(start+fieldLength) == 1) {
                return null;
            }
            return getString(buffer, start, fieldLength, 1);
        };
    }

    public static Function<ByteBuffer, String> NewWStringExtractor(int start, int fieldLength) {
        return (buffer) -> {
            if (buffer.get(start + (fieldLength*2))==1) {
                return null;
            }
            return getString(buffer, start, fieldLength, 2);
        };
    }

    public static Function<ByteBuffer, byte[]> NewBlobExtractor(int start) {
        return (buffer) -> {
            var fixedPortion = buffer.getInt(start);
            if (fixedPortion == 0) {
                return new byte[]{};
            }
            if (fixedPortion == 1) {
                return null;
            }

            if (isTiny(fixedPortion)) {
                return getTinyBlob(start, buffer);
            }

            var blockStart = start + (fixedPortion & 0x7fffffff);
            var blockFirstByte = buffer.get(blockStart);
            if (isSmallBlock(blockFirstByte)) {
                return getSmallBlob(buffer, blockStart);
            } else {
                return getNormalBlob(buffer, blockStart);
            }
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

    private static String getString(ByteBuffer buffer, int start, int fieldLength, int charSize) {
        int end = getEndOfStringPos(buffer, start, fieldLength, charSize);
        if (charSize == 1) {
            return new String(Arrays.copyOfRange(buffer.array(), start, end), StandardCharsets.UTF_8);
        } else {
            return new String(Arrays.copyOfRange(buffer.array(), start, end), StandardCharsets.UTF_16LE);
        }
    }

    private static int getEndOfStringPos(ByteBuffer buffer, int start, int fieldLength, int charSize) {
        int fieldTo = start + (fieldLength * charSize);
        int strLen = 0;
        for (var i = start; i < fieldTo; i=i+charSize) {
            if (buffer.get(i) == 0 && buffer.get(i+(charSize-1)) == 0) {
                break;
            }
            strLen++;
        }
        return start+(strLen * charSize);
    }

    private static boolean isTiny(int fixedPortion) {
        var bitCheck1 = fixedPortion & 0x80000000;
        var bitCheck2 = fixedPortion & 0x30000000;
        return bitCheck1 == 0 && bitCheck2 != 0;
    }

    private static boolean isSmallBlock(byte value) {
        return (value & 1) == 1;
    }

    private static byte[] getNormalBlob(ByteBuffer buffer, int blockStart) {
        var blobLen = buffer.getInt(blockStart) / 2; // why divided by 2? not sure
        var blobStart = blockStart + 4;
        var blobEnd = blobStart + blobLen;
        return Arrays.copyOfRange(buffer.array(), blobStart, blobEnd);
    }

    private static byte[] getSmallBlob(ByteBuffer buffer, int blockStart) {
        var blockFirstByte = buffer.get(blockStart);
        var blobLen = unsign(blockFirstByte) >> 1;
        var blobStart = blockStart + 1;
        var blobEnd = blobStart + blobLen;
        return Arrays.copyOfRange(buffer.array(), blobStart, blobEnd);
    }

    private static byte[] getTinyBlob(int start, ByteBuffer buffer) {
        var intVal = buffer.getInt(start);
        var length = intVal >> 28;
        var end = start + length;
        return Arrays.copyOfRange(buffer.array(), start, end);
    }

    private static int unsign(byte value) {
        return value & 0xff; // Java's bytes are signed while the original algorithm is written for unsigned bytes
    }
}

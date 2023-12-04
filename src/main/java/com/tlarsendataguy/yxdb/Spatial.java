package com.tlarsendataguy.yxdb;


import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * Spatial contains a static function to translate SpatialObj fields into GeoJSON.
 */
public class Spatial {
    final static private int BytesPerPoint = 16;

    /**
     * ToGeoJson translates SpatialObj fields into GeoJSON.
     * <p>
     * Alteryx stores spatial objects in a binary format. This function reads the binary format and converts it to a GeoJSON string.
     * @param value The object read from a SpatialObj field
     * @return A GeoJSON string representing the spatial object
     * @throws IllegalArgumentException The blob is not a valid spatial object
     */
    public static String ToGeoJson(byte[] value) throws IllegalArgumentException {
        if (value == null) {
            return "";
        }
        if (value.length < 20) {
            throw new IllegalArgumentException("bytes are not a spatial object");
        }
        var buffer = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN);
        var objType = buffer.getInt(0);
        switch (objType) {
            case 8 -> {
                return ParsePoints(buffer);
            }
            case 3 -> {
                return ParseLines(buffer);
            }
            case 5 -> {
                return ParsePoly(buffer);
            }
        }
        throw new IllegalArgumentException("bytes are not a spatial object");
    }

    private static String ParsePoints(ByteBuffer value) {
        var totalPoints = value.getInt(36);
        if (totalPoints == 1) {
            return ParseSinglePoint(value);
        }
        return ParseMultiPoint(value);
    }

    private static String ParseSinglePoint(ByteBuffer value) {
        return GeoJson("Point", GetCoordAt(value, 40));
    }

    private static String ParseMultiPoint(ByteBuffer value) {
        var points = new ArrayList<double[]>();
        var i = 40;
        while (i < value.capacity()) {
            points.add(GetCoordAt(value, i));
            i += BytesPerPoint;
        }
        return GeoJson("MultiPoint", points);
    }

    private static String ParseLines(ByteBuffer value) {
        var lines = ParseMultiPointObjects(value);

        if (lines.size() == 1) {
            return GeoJson("LineString", lines.get(0));
        }
        return GeoJson("MultiLineString", lines);
    }

    private static String ParsePoly(ByteBuffer value) {
        var poly = ParseMultiPointObjects(value);

        if (poly.size() == 1) {
            return GeoJson("Polygon", poly);
        }
        var coordinates = new ArrayList<ArrayList<ArrayList<double[]>>>();
        coordinates.add(poly);
        return GeoJson("MultiPolygon", coordinates);
    }

    private static ArrayList<ArrayList<double[]>> ParseMultiPointObjects(ByteBuffer value) {
        var endingIndices = GetEndingIndices(value);

        var i = 48 + (endingIndices.length * 4) - 4;
        var objects = new ArrayList<ArrayList<double[]>>();
        for (var endingIndex : endingIndices) {
            var line = new ArrayList<double[]>();
            while (i < endingIndex) {
                line.add(GetCoordAt(value, i));
                i += BytesPerPoint;
            }
            objects.add(line);
        }
        return objects;
    }

    private static int[] GetEndingIndices(ByteBuffer value) {
        var totalObjects = value.getInt(36);
        var totalPoints = (int)value.getLong(40);
        var endingIndices = new int[totalObjects];

        var i = 48;
        var startAt = 48 + ((totalObjects - 1) * 4);
        for (var j = 1; j < totalObjects; j++) {
            var endingPoint = value.getInt(i);
            var endingIndex = (endingPoint * BytesPerPoint) + startAt;
            endingIndices[j-1] = endingIndex;
            i += 4;
        }
        endingIndices[totalObjects-1] = (totalPoints*BytesPerPoint)+startAt;
        return endingIndices;
    }

    private static double[] GetCoordAt(ByteBuffer value, int at) {
        var lng = value.getDouble(at);
        var lat = value.getDouble(at+8);
        return new double[]{lng, lat};
    }

    private static String GeoJson(String objType, Object coordinates) {
        var builder = new StringBuilder();
        builder.append("{\"type\":\"");
        builder.append(objType);
        builder.append("\",\"coordinates\":");
        CoordinatesToJson(builder, coordinates);
        builder.append('}');
        return builder.toString();
    }

    private static void CoordinatesToJson(StringBuilder builder, Object coordinates) {
        if (coordinates instanceof List items) {
            builder.append('[');
            var first = true;
            for (var item : items) {
                if (!first) {
                    builder.append(',');
                }
                CoordinatesToJson(builder, item);
                first = false;
            }
            builder.append(']');
            return;
        }
        if (coordinates instanceof double[] items) {
            builder.append('[');
            var first = true;
            for (var item : items) {
                if (!first) {
                    builder.append(',');
                }
                CoordinatesToJson(builder, item);
                first = false;
            }
            builder.append(']');
            return;
        }
        builder.append(coordinates);
    }
}

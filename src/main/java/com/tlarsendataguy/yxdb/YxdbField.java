package com.tlarsendataguy.yxdb;

/**
 * Contains field information parsed from .yxdb metadata.
 * @param name the name of the field
 * @param type the data type contained in the field. The type identifies which read functions should be used in YxdbReader to access this field's contents
 * @see DataType
 */
public record YxdbField(String name, DataType type) {
    /**
     * Fields can contain one of the following types of data. All fields types may return nulls.
     * <ul>
     *     <li>BLOB: an array of bytes</li>
     *     <li>BOOLEAN: boolean values</li>
     *     <li>BYTE: a single byte</li>
     *     <li>DATE: either date or datetime values</li>
     *     <li>DOUBLE: numbers</li>
     *     <li>LONG: integers</li>
     *     <li>STRING: text</li>
     * </ul>
     */
    public enum DataType {
        BLOB, BOOLEAN, BYTE, DATE, DOUBLE, LONG, STRING
    }
}

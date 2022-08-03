package com.tlarsendataguy.yxdb;

public record YxdbField(String name, DataType type) {
    public enum DataType {
        BLOB, BOOLEAN, BYTE, DATE, DOUBLE, LONG, STRING
    }
}

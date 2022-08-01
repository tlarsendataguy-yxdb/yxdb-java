package com.tlarsendataguy.yxdb;

public record YxdbField(String name, DataType type) {
    public enum DataType {
        BOOLEAN, BYTE, LONG, DOUBLE, STRING, DATE, BLOB
    }
}

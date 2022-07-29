package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import java.io.FileNotFoundException;

public class ReaderTest {
    @Test
    public void TestGetReader() {
        var path = "src/test/resources/AllNormalFields.yxdb";
        try{
            var yxdb = Reader.loadYxdb(path);
            Assertions.assertEquals(1, yxdb.numRecords);
        } catch (Exception ex){
            Assertions.fail(ex.toString());
        }
    }
}

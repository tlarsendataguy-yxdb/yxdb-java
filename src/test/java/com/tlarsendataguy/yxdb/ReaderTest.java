package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

public class ReaderTest {
    @Test
    public void TestGetReader() {
        Reader reader = new Reader();
        Assertions.assertNotNull(reader);
    }
}

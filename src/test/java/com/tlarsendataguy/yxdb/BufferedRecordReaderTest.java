package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BufferedRecordReaderTest {
    @Test
    public void TestLotsOfRecords() throws IOException {
        var reader = generateReader("src/test/resources/LotsOfRecords.yxdb", 5, false);

        int recordsRead = 0;
        while (reader.nextRecord()) {
            recordsRead++;
            Assertions.assertEquals(recordsRead, reader.recordBuffer.getInt(0));
        }
        Assertions.assertEquals(100000, recordsRead);
    }

    @Test
    public void TestVeryLongFieldFile() throws IOException {
        var reader = generateReader("src/test/resources/VeryLongField.yxdb",6, true);

        int recordsRead = 0;
        while (reader.nextRecord()) {
            recordsRead++;
            Assertions.assertEquals(recordsRead, reader.recordBuffer.get(0));
        }
        Assertions.assertEquals(3, recordsRead);
    }

    private BufferedRecordReader generateReader(String path, int fixedLen, boolean hasVarFields) throws IOException {
        var stream = new FileInputStream(path);
        var header = ByteBuffer.allocate(512).order(ByteOrder.LITTLE_ENDIAN);
        stream.readNBytes(header.array(), 0, 512);
        var metaInfoSize = header.getInt(80) * 2;
        var totalRecords = header.getLong(104);
        stream.skip(metaInfoSize);
        return new BufferedRecordReader(stream,fixedLen, hasVarFields, totalRecords);
    }
}

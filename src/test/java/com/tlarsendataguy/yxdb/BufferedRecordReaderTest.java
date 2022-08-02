package com.tlarsendataguy.yxdb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BufferedRecordReaderTest {
    @Test
    public void TestAllNormalFieldsFile() throws IOException {
        var reader = generateReader("src/test/resources/LotsOfRecords.yxdb");

        int recordsRead = 0;
        while (reader.nextRecord()) {
            recordsRead++;
            Assertions.assertEquals(recordsRead, reader.recordBuffer.getInt(0));
        }
        Assertions.assertEquals(100000, recordsRead);
    }

    private BufferedRecordReader generateReader(String path) throws IOException {
        var stream = new FileInputStream(path);
        var header = ByteBuffer.allocate(512).order(ByteOrder.LITTLE_ENDIAN);
        stream.readNBytes(header.array(), 0, 512);
        var metaInfoSize = header.getInt(80) * 2;
        stream.skip(metaInfoSize);
        return new BufferedRecordReader(stream,5, false, 100000);
    }
}

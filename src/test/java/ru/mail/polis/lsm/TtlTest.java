package ru.mail.polis.lsm;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;

import static ru.mail.polis.lsm.Utils.generateMap;

public class TtlTest {
    private DAO dao;

    @BeforeEach
    void start(@TempDir Path dir) throws IOException {
        dao = TestDaoWrapper.create(new DAOConfig(dir));
    }

    @AfterEach
    void finish() throws IOException {
        dao.close();
    }

    @Test
    void simpleExpirationTest() {
        try {
            long startTime = System.currentTimeMillis();
            long ttl = 300;

            Map<ByteBuffer, ByteBuffer> map = generateMap(0, 1000);
            map.forEach((k, v) -> dao.upsert(Record.of(k, v), ttl));

            Thread.sleep(ttl);

            long finishTime = System.currentTimeMillis();
            Assertions.assertTrue(finishTime - startTime > ttl);

            Iterator<Record> records = dao.range(null, null);
            Assertions.assertFalse(records.hasNext());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    
}

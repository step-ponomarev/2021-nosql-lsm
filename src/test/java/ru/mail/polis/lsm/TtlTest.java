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
import java.util.concurrent.ThreadLocalRandom;

import static ru.mail.polis.lsm.Utils.assertDaoEquals;
import static ru.mail.polis.lsm.Utils.generateMap;

class TtlTest {
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
    void baseExpirationTest() {
        try {
            long startTime = System.currentTimeMillis();
            long ttl = 300;

            Map<ByteBuffer, ByteBuffer> map = generateMap(0, 120);
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

    @Test
    void deathSquadTest() {
        Map<ByteBuffer, ByteBuffer> map = generateMap(0, 100);
        map.forEach((k, v) -> dao.upsert(Record.of(k, v), 0L));

        Iterator<Record> records = dao.range(null, null);
        Assertions.assertFalse(records.hasNext());
    }

    @Test
    void immortalSquad() {
        Map<ByteBuffer, ByteBuffer> map = generateMap(0, 100);
        map.forEach((k, v) -> dao.upsert(Record.of(k, v), Long.MAX_VALUE));

        assertDaoEquals(dao, map);
    }

    @Test
    void negativeTimeTest() {
        Map<ByteBuffer, ByteBuffer> map = generateMap(0, 1);

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            map.forEach((k, v) -> dao.upsert(Record.of(k, v), -1));
        });
    }

    @Test
    void mixedUpsertTest() throws IOException {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final Map<ByteBuffer, ByteBuffer> map = generateMap(0, 3000);
        final long sleepTime = 250;

        int shouldDieAmount = 0;
        long ttl;
        for (Map.Entry<ByteBuffer, ByteBuffer> record : map.entrySet()) {
            ttl = random.nextBoolean() ? random.nextLong(500) : 0;

            if (ttl >= sleepTime || ttl == 0) {
                shouldDieAmount++;
            }

            dao.upsert(Record.of(record.getKey(), record.getValue()), ttl);
        }

        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Iterator<Record> records = dao.range(null, null);

        int liveAmount = 0;
        while (records.hasNext()) {
            records.next();
            liveAmount++;
        }

        Assertions.assertTrue(shouldDieAmount <= map.size() - liveAmount);
    }
}

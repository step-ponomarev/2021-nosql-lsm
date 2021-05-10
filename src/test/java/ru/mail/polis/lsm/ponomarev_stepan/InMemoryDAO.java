package ru.mail.polis.lsm.ponomarev_stepan;

import java.util.Collection;
import java.util.Iterator;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.SortedMap;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDAO implements DAO {
    private final SortedMap<ByteBuffer, Record> store = new ConcurrentSkipListMap<>();

    @Override
    public synchronized Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null || store.isEmpty()) {
            return store.values().iterator();
        }

        return selectData(fromKey, toKey).iterator();
    }

    @Override
    public synchronized void upsert(Record record) {
        var key = record.getKey();
        var value = record.getValue();

        if (value == null) {
            store.remove(key);
        } else {
            store.put(key, Record.of(key, value));
        }
    }

    @Override
    public synchronized void close() throws IOException {
        store.clear();
    }

    private Collection<Record> selectData(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        final boolean selectFromHead = fromKey == null;
        final boolean selectTillEnd = toKey == null;

        return (selectFromHead ? store.headMap(toKey)
                : selectTillEnd ? store.tailMap(fromKey)
                : store.subMap(fromKey, toKey))
                .values();
    }
}

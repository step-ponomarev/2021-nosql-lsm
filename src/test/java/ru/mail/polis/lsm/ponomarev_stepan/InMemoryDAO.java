package ru.mail.polis.lsm.ponomarev_stepan;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MMMDAO implements DAO {
    private final SortedMap<ByteBuffer, Record> store = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null || store.isEmpty()) {
            return store.values().iterator();
        }

        return (fromKey == null ? store.headMap(toKey)
                : toKey == null ? store.tailMap(fromKey)
                : store.subMap(fromKey, toKey))
                .values()
                .iterator();
    }

    @Override
    public void upsert(Record record) {
        var key = record.getKey();
        var value = record.getValue();

        if (value == null) {
            store.remove(key);
        } else {
            store.put(key, Record.of(key, value));
        }
    }

    @Override
    public void close() throws IOException {
        store.clear();
    }
}

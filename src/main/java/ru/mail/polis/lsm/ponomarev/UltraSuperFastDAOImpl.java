package ru.mail.polis.lsm.ponomarev;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class UltraSuperFastDAOImpl implements DAO {
    private static final int MEMORY_LIMIT = 123456;

    private final NavigableMap<ByteBuffer, Record> store;
    private final SSTable table;

    /**
     * @param config конфигурация дао
     * @throws IOException выбрасывает в случае чего вдруг, такое возможно
     */
    public UltraSuperFastDAOImpl(DAOConfig config) throws IOException {
        this.table = new SSTable(config.getDir());
        this.store = new ConcurrentSkipListMap<>();

        var records = this.table.read(null, null);
        while (records.hasNext()) {
            var record = records.next();
            if (record.isTombstone()) {
                store.remove(record.getKey());
            } else {
                store.put(record.getKey(), record);
            }
        }
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        try {
            var merged = DAO.merge(List.of(table.read(fromKey, toKey), store.values()
                    .stream()
                    .filter(r -> filterRecords(r, fromKey, toKey))
                    .iterator())
            );

            return merged;
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyIterator();
        }
    }

    private boolean filterRecords(Record record, ByteBuffer fromKey, ByteBuffer toKey) {
        if (record.isTombstone()) {
            return false;
        }

        if (fromKey == null && toKey == null) {
            return true;
        }

        if (fromKey == null) {
            return record.getKey().compareTo(toKey) <= 0;
        }

        if (toKey == null) {
            return record.getKey().compareTo(fromKey) >= 0;
        }

        return record.getKey().compareTo(fromKey) >= 0
                && record.getKey().compareTo(toKey) <= 0;
    }

    @Override
    public void upsert(Record record) {
        ByteBuffer key = record.getKey();
        ByteBuffer value = record.getValue();

        Record newRecord = value == null ? Record.tombstone(key) : Record.of(key, value);
        store.put(key, newRecord);
    }

    @Override
    public void close() throws IOException {
        table.flush(store.values());
    }
}

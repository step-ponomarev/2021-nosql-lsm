package ru.mail.polis.lsm.ponomarev;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
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
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return this.store.values().stream().filter(r -> !r.isTombstone()).iterator();
        }

        return this.store.values()
                .stream()
                .filter(r -> recordFilter(r, fromKey, toKey))
                .iterator();
    }

    private boolean recordFilter(Record record, ByteBuffer fromKey, ByteBuffer toKey) {
        if (record.isTombstone()) {
            return false;
        }

        return fromKey == null
                ? record.getKey().compareTo(toKey) <= 0
                : record.getKey().compareTo(fromKey) >= 0;
    }

    @Override
    public void upsert(Record record) {
        ByteBuffer key = record.getKey();
        ByteBuffer value = record.getValue();

        Record newRecord = value == null ? Record.tombstone(key) : Record.of(key, value);

        this.store.put(key, newRecord);
    }

    @Override
    public void close() throws IOException {
        this.store.clear();
    }
}

package ru.mail.polis.lsm.ponomarev;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

public class PonomarevDAO implements DAO {
    private static final int MEMORY_LIMIT = 131072;

    private final NavigableMap<ByteBuffer, Record> store;
    private final AtomicInteger storeSize;
    private final SSTable sstable;

    /**
     * @param config конфигурация дао
     * @throws IOException выбрасывает в случае чего вдруг, такое возможно
     */
    public PonomarevDAO(DAOConfig config) throws IOException {
        this.store = new ConcurrentSkipListMap<>();
        this.storeSize = new AtomicInteger(0);
        this.sstable = new SSTable(config.getDir());
    }

    @Override
    public void upsert(Record record) {
        try {
            var key = record.getKey();
            var value = record.getValue();

            if (value != null) {
                store.put(key, Record.of(key, value));
            } else {
                store.put(key, Record.tombstone(key));
            }

            storeSize.getAndAdd(sizeOf(record));
            
            if (storeSize.get() >= MEMORY_LIMIT) {
                sstable.flush(store);

                store.clear();
                storeSize.set(0);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Disk is not available", e);
        }
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> data = sstable.range(fromKey, toKey);
        data.add(store.values().iterator());

        if (data.isEmpty()) {
            return Collections.emptyIterator();
        }
        
        return filterData(data, fromKey, toKey);
    }

    @Override
    public void close() throws IOException {
        sstable.flush(store);
    }

    private Iterator<Record> filterData(List<Iterator<Record>> data, ByteBuffer fromKey, ByteBuffer toKey) {
        return StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(DAO.merge(data), Spliterator.ORDERED), false)
                .filter(r -> filterByKeys(r, fromKey, toKey))
                .iterator();
    }

    private boolean filterByKeys(Record record, ByteBuffer fromKey, ByteBuffer toKey) {
        if (record.getValue() == null) {
            return false;
        }

        boolean valid = true;

        if (fromKey != null) {
            valid = record.getKey().compareTo(fromKey) >= 0;
        }

        if (toKey != null) {
            valid &= record.getKey().compareTo(toKey) <= 0;
        }

        return valid;
    }

    private int sizeOf(Record record) {
        return record.getKey().remaining() + ((record.getValue() == null) ? 0 : record.getValue().remaining());
    }
}

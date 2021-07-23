package ru.mail.polis.lsm.ponomarev;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class FasterThanPrevDAOImpl implements DAO {
    private static final int MEMORY_LIMIT = 9999999;

    public class RecordWithMetaData {
        @Nonnull
        private final Record record;
        private final Long timeToLive;

        public RecordWithMetaData(@Nonnull Record record) {
            this(record, null);
        }

        public RecordWithMetaData(@Nonnull Record record, @Nullable Long timeToLive) {
            this.record = record;
            this.timeToLive = timeToLive;
        }

        public boolean isMortal() {
            return timeToLive == null;
        }

        @Nonnull
        public Record getRecord() {
            return record;
        }

        @Nullable
        public Long getTimeToLive() {
            return timeToLive;
        }
    }

    private NavigableMap<ByteBuffer, RecordWithMetaData> store;
    private final SSTable table;
    private volatile int storeSize;

    /**
     * @param config конфигурация дао
     */
    public FasterThanPrevDAOImpl(DAOConfig config) {
        this.table = new SSTable(config.getDir());
        this.store = new ConcurrentSkipListMap<>();
        this.storeSize = 0;
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        try {
            var recordsFromDisk = table.read(fromKey, toKey);
            var recordsFromStore = getStoredValues(fromKey, toKey);

            return DAO.merge(List.of(recordsFromDisk, recordsFromStore));
        } catch (IOException e) {
            throw new RuntimeException("Disk reading are failed.", e.getCause());
        }
    }

    @Override
    public void upsert(Record record) {
        try {
            ByteBuffer key = record.getKey();
            ByteBuffer value = record.getValue();

            Record newRecord = value == null ? Record.tombstone(key) : Record.of(key, value);
            store.put(key, new RecordWithMetaData(record));

            updateStoreSize(newRecord);

            if (storeSize >= MEMORY_LIMIT) {
                synchronized (this) {
                    if (storeSize >= MEMORY_LIMIT) {
                        flushRecords();
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Store flushing are failed.", e.getCause());
        }
    }

    @Override
    public void upsert(Record record, long timeToLive) {
        try {
            ByteBuffer key = record.getKey();
            ByteBuffer value = record.getValue();

            Record newRecord = value == null ? Record.tombstone(key) : Record.of(key, value);
            store.put(key, new RecordWithMetaData(record, timeToLive));

            updateStoreSize(newRecord);

            if (storeSize >= MEMORY_LIMIT) {
                synchronized (this) {
                    if (storeSize >= MEMORY_LIMIT) {
                        flushRecords();
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Store flushing are failed.", e.getCause());
        }
    }

    @Override
    public void close() throws IOException {
        table.flush(store.values().iterator());
        store = null;
    }

    private Iterator<Record> getStoredValues(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return store.values()
                .stream()
                .filter(r -> filterRecords(r.getRecord(), fromKey, toKey))
                .map(RecordWithMetaData::getRecord)
                .iterator();
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

    private void flushRecords() throws IOException {
        table.flush(store.values().iterator());
        store = new ConcurrentSkipListMap<>();
        storeSize = 0;
    }

    private synchronized void updateStoreSize(Record record) {
        storeSize += sizeOf(record);
    }

    private int sizeOf(Record record) {
        return record.getKey().remaining() + (record.isTombstone() ? 0 : record.getValue().remaining());
    }
}

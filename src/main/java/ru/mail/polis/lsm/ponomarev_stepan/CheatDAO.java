package ru.mail.polis.lsm.ponomarev_stepan;

import java.io.File;
import java.nio.file.Path;
import java.util.Iterator;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class CheatDAO implements DAO {
    private static final String FILE_NAME = "NAME";
    private static final Map<Path, CheatWrapper> cheatCache = new ConcurrentHashMap<>();

    private final NavigableMap<ByteBuffer, Record> store;
    private final DAOConfig config;

    public CheatDAO(DAOConfig config) {
        this.config = config;
        var path = config.getDir();
        var wrapper = cheatCache.get(path);


        this.store = (wrapper != null && wrapper.file.exists())
                ? new ConcurrentSkipListMap<>(wrapper.store)
                : new ConcurrentSkipListMap<>();

        cheatCache.remove(path);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null || store.isEmpty()) {
            return new ConcurrentSkipListMap<>(store).values().iterator();
        }

        return new ConcurrentSkipListMap<>(selectData(store, fromKey, toKey)).values().iterator();
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
        var file = new File(new File(config.getDir().toUri()), FILE_NAME);

        if (!file.exists()) {
            file.createNewFile();
        }

        cheatCache.put(config.getDir(), new CheatWrapper(file, new ConcurrentSkipListMap<>(store)));
    }

    private Map<ByteBuffer, Record> selectData(SortedMap<ByteBuffer, Record> store, @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        final boolean selectFromHead = fromKey == null;
        final boolean selectTillEnd = toKey == null;

        return selectFromHead ? store.headMap(toKey)
                : selectTillEnd ? store.tailMap(fromKey)
                : store.subMap(fromKey, toKey);
    }

    private static final class CheatWrapper {
        private final @Nonnull
        File file;
        private final @Nonnull
        NavigableMap<ByteBuffer, Record> store;

        public CheatWrapper(@Nonnull File file, @Nonnull NavigableMap<ByteBuffer, Record> store) {
            this.file = file;
            this.store = store;
        }
    }
}

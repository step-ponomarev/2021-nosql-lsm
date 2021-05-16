package ru.mail.polis.lsm.ponomarev_stepan;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Iterator;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentSkipListMap;

public class SimpleMemoryFileDAO implements DAO {
    private static final String FILE_NAME = "NAME";

    private final NavigableMap<ByteBuffer, Record> store;
    private final File file;

    public SimpleMemoryFileDAO(DAOConfig config) {
        this.file = new File(new File(config.getDir().toUri()), FILE_NAME);
        this.store = read(file);
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
        writeByteBuffer();
        this.store.clear();
    }

    private Map<ByteBuffer, Record> selectData(SortedMap<ByteBuffer, Record> store, @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        final boolean selectFromHead = fromKey == null;
        final boolean selectTillEnd = toKey == null;

        return selectFromHead ? store.headMap(toKey)
                : selectTillEnd ? store.tailMap(fromKey)
                : store.subMap(fromKey, toKey);
    }

    private NavigableMap<ByteBuffer, Record> read(File file) {
        if (!file.exists()) {
            return new ConcurrentSkipListMap<>();
        }

        NavigableMap<ByteBuffer, Record> tmpStore = new ConcurrentSkipListMap<>();
        try (BufferedInputStream is = new BufferedInputStream(new FileInputStream(file))) {
            while (is.available() != 0) {
                ByteBuffer key = readByteBuffer(is);
                ByteBuffer value = readByteBuffer(is);

                tmpStore.put(key, Record.of(key, value));
            }
        } catch (IOException e) {
            return new ConcurrentSkipListMap<>();
        }

        return tmpStore;
    }

    private ByteBuffer readByteBuffer(BufferedInputStream is) throws IOException {
        int length = is.read();
        return ByteBuffer.wrap(is.readNBytes(length));
    }

    private void writeByteBuffer() throws IOException {
        try (BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(file, false))) {
            if (!file.exists()) {
                file.createNewFile();
            }

            for (var entry : store.entrySet()) {
                writeByteBuffer(os, entry.getKey());
                writeByteBuffer(os, entry.getValue().getValue());
            }
        }
    }

    private void writeByteBuffer(BufferedOutputStream os, ByteBuffer buffer) throws IOException {
        var byteArray = toByteArray(buffer);
        os.write(byteArray.length);
        os.write(byteArray);
    }

    private byte[] toByteArray(ByteBuffer buffer) {
        int length = buffer.remaining();
        byte[] bytes = new byte[length];
        buffer.get(bytes);

        return bytes;
    }
}

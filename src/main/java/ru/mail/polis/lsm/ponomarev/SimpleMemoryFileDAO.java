package ru.mail.polis.lsm.ponomarev;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class SimpleMemoryFileDAO implements DAO {
    private static final String FILE_NAME = "file.file";
    private static final Set<? extends OpenOption> READ_OPEN_OPTIONS = EnumSet.of(StandardOpenOption.READ);
    private static final Set<? extends OpenOption> WRITE_OPEN_OPTIONS
            = EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
    
    private final NavigableMap<ByteBuffer, Record> store;
    private final DAOConfig config;

    public SimpleMemoryFileDAO(DAOConfig config) throws IOException {
        this.config = config;
        this.store = read(config.getDir().resolve(FILE_NAME));
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
        var path = config.getDir().resolve(FILE_NAME);
        Files.deleteIfExists(path);
        writeByteBuffer(path);

        this.store.clear();
    }

    private Map<ByteBuffer, Record> selectData(SortedMap<ByteBuffer, Record> store,
                                               @Nullable ByteBuffer fromKey,
                                               @Nullable ByteBuffer toKey) {
        final var selectFromHead = fromKey == null;
        if (selectFromHead) {
            return store.headMap(toKey);
        }

        final var selectTillEnd = toKey == null;
        if (selectTillEnd) {
            return store.tailMap(fromKey);
        }

        return store.subMap(fromKey, toKey);
    }

    private NavigableMap<ByteBuffer, Record> read(Path path) throws IOException {
        if (Files.notExists(path)) {
            return new ConcurrentSkipListMap<>();
        }

        NavigableMap<ByteBuffer, Record> tmpStore = new ConcurrentSkipListMap<>();
        try (var fc = FileChannel.open(path, READ_OPEN_OPTIONS)) {
            var mappedBuffer = fc.map(FileChannel.MapMode.READ_ONLY, 0_0, fc.size());

            while (mappedBuffer.hasRemaining()) {
                var key = readByteBuffer(mappedBuffer);
                var value = readByteBuffer(mappedBuffer);

                tmpStore.put(key, Record.of(key, value));
            }
        }

        return tmpStore;
    }

    private ByteBuffer readByteBuffer(MappedByteBuffer mappedBuffer) {
        var size = mappedBuffer.getInt();
        var buffer = mappedBuffer.slice().limit(size).asReadOnlyBuffer();
        mappedBuffer.position(mappedBuffer.position() + size);

        return buffer;
    }

    private void writeByteBuffer(Path path) throws IOException {
        try (var os = FileChannel.open(path, WRITE_OPEN_OPTIONS)) {
            for (var entry : store.entrySet()) {
                var record = entry.getValue();

                writeByteBuffer(os, record.getKey());
                writeByteBuffer(os, record.getValue());
            }
        }
    }

    private void writeByteBuffer(FileChannel os, ByteBuffer buffer) throws IOException {
        os.write(getBufferSize(buffer));
        os.write(buffer);
    }

    private ByteBuffer getBufferSize(ByteBuffer buffer) {
        return ByteBuffer.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(buffer.remaining()).array());
    }
}

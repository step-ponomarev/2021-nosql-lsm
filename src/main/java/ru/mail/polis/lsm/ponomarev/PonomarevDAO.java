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
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.StreamSupport;

public class PonomarevDAO implements DAO {
    private static final Comparator<Index> indexComparator = Comparator.comparing(i -> i.order);

//    private static final long MEMORY_LIMIT = 128 * 1024 * 1024 / 4;

    private static final String INDEXES_FILE_NAME = "index.info.dat";

    private static final int FILE_RECORD_LIMIT = 1000;
    private static final Set<? extends OpenOption> READ_OPEN_OPTIONS = EnumSet.of(StandardOpenOption.READ);
    private static final Set<? extends OpenOption> WRITE_OPTIONS
            = EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE_NEW);

    private final DAOConfig config;
    private final Map<Integer, Index> indexes;

    private Index minIndex;
    private Index maxIndex;

    private static class Index {
        public final int order;
        public ByteBuffer startKey;
        public int recordAmount;

        public Index(int order, ByteBuffer startKey, int recordAmount) {
            this.order = order;
            this.startKey = startKey;
            this.recordAmount = recordAmount;
        }
    }

    public PonomarevDAO(DAOConfig config) throws IOException {
        this.config = config;
        this.indexes = readIndexes(READ_OPEN_OPTIONS);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (indexes.isEmpty()) {
            return Collections.emptyIterator();
        }
        
        try {
            if (fromKey == null && toKey == null) {
                synchronized (this) {
                    List<Iterator<Record>> iterators = new ArrayList<>();

                    for (var i : indexes.values()) {
                        var path = getPath(getFileName(i.order));
                        if (Files.exists(path)) {
                            var iterator = read(path, READ_OPEN_OPTIONS);
                            iterators.add(iterator);
                        }
                    }

                    return DAO.merge(iterators);
                }
            }

            List<Iterator<Record>> data = new ArrayList<>();
            if (fromKey == null) {
                data.addAll(getDataFromIndex(minIndex));
            } else {
                data.add(filterStartData(getDataFromIndex(findIndex(fromKey)), fromKey));
            }

            if (data.isEmpty()) {
                return Collections.emptyIterator();
            }

            if (toKey == null) {
                return DAO.merge(data);
            } else {
                return filterEndData(data, toKey);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Something wrong", e);
        }
    }

    private Iterator<Record> filterStartData(List<Iterator<Record>> data, ByteBuffer from) {
        return StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(DAO.merge(data), Spliterator.ORDERED), false)
                .filter(f -> f.getKey().compareTo(from) >= 0)
                .iterator();
    }

    private Iterator<Record> filterEndData(List<Iterator<Record>> data, ByteBuffer toKey) {
        return StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(DAO.merge(data), Spliterator.ORDERED), false)
                .filter(f -> f.getKey().compareTo(toKey) <= 0)
                .iterator();
    }

    @Override
    public void upsert(Record record) {
        var key = record.getKey();
        var value = record.getValue();

        resolveMinMaxIndexes(key);

        var newRecord = value != null ? Record.of(key, value) : Record.tombstone(key);
        try {
            flush(newRecord);
            writeIndexes(WRITE_OPTIONS);
        } catch (IOException e) {
            throw new IllegalStateException("Something wrong");
        }
    }

    private void resolveMinMaxIndexes(ByteBuffer key) {
        Index index = null;

        if (minIndex == null || maxIndex == null) {
            index = new Index(0, key, 0);

            maxIndex = index;
            minIndex = index;
        } else if (key.compareTo(minIndex.startKey) < 0) {
            if (maxIndex.recordAmount == FILE_RECORD_LIMIT) {
                index = new Index(minIndex.order - 1, key, 0);
            } else {
                index = new Index(minIndex.order, key, minIndex.recordAmount);
            }

            minIndex = index;
        } else if (key.compareTo(maxIndex.startKey) > 0) {
            if (maxIndex.recordAmount == FILE_RECORD_LIMIT) {
                index = new Index(maxIndex.order + 1, key, 0);
            } else {
                index = new Index(maxIndex.order, key, maxIndex.recordAmount);
            }

            maxIndex = index;
        }

        if (index != null) {
            indexes.put(index.order, index);
        }
    }

    private Index findIndex(ByteBuffer key) {
        List<Index> indexList = new ArrayList<>(indexes.values());

        while (indexList.size() > 1) {
            var i = indexList.size() / 2;
            var midIndex = indexList.get(i);

            var compareResult = key.compareTo(midIndex.startKey);

            if (compareResult == 0) {
                return midIndex;
            }

            if (compareResult < 0) {
                var prevIndex = indexList.get(i - 1);
                if (key.compareTo(prevIndex.startKey) >= 0) {
                    return prevIndex;
                }

                indexList = indexList.subList(0, i - 1);
            }

            if (compareResult > 0) {
                var nextIndex = indexList.get(i + 1);
                if (key.compareTo(nextIndex.startKey) <= 0) {
                    return midIndex;
                }

                indexList = indexList.subList(0, i - 1);
            }
        }

        if (indexList.isEmpty()) {
            throw new IllegalStateException("Something wrong");
        }

        return indexList.get(0);
    }

    @Override
    public void close() throws IOException {
        writeIndexes(WRITE_OPTIONS);
    }

    private void writeIndexes(final Set<? extends OpenOption> options) throws IOException {
        var path = getPath(INDEXES_FILE_NAME);
        Files.deleteIfExists(path);

        try (var fc = FileChannel.open(path, options)) {
            var mappedBuffer = fc.map(FileChannel.MapMode.READ_WRITE, 0, getIndexesSize());

            for (var index : indexes.values()) {
                mappedBuffer.putInt(index.order);
                mappedBuffer.putInt(index.startKey.remaining());
                mappedBuffer.put(index.startKey);
                mappedBuffer.putInt(index.recordAmount);
            }
        }
    }

    private int getIndexesSize() {
        return this.indexes.isEmpty() ? 0 : this.indexes.values()
                .stream()
                .map(i -> 4 * 2 + i.startKey.remaining() + 4)
                .reduce(Integer::sum)
                .orElseThrow();
    }

    private Map<Integer, Index> readIndexes(final Set<? extends OpenOption> options) throws IOException {
        var path = getPath(INDEXES_FILE_NAME);

        if (Files.notExists(path)) {
            return new ConcurrentSkipListMap<>();
        }

        final Map<Integer, Index> indexesTmp = new ConcurrentSkipListMap();
        try (var fc = FileChannel.open(path, options)) {
            var mappedBuffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
            while (mappedBuffer.hasRemaining()) {
                var order = mappedBuffer.getInt();
                var key = readByteBuffer(mappedBuffer);
                var amount = mappedBuffer.getInt();

                var index = new Index(order, key, amount);
                indexesTmp.put(index.order, index);

                if (minIndex == null && maxIndex == null) {
                    minIndex = index;
                    maxIndex = index;
                }

                if (indexComparator.compare(index, minIndex) < 0) {
                    minIndex = index;
                }

                if (indexComparator.compare(index, maxIndex) > 0) {
                    maxIndex = index;
                }
            }
        }

        return indexesTmp;
    }

    private void flush(Record record) throws IOException {
        var index = findIndex(record.getKey());
        var path = getPath(getFileName(index.order));
        var data = Files.exists(path) ? read(path, READ_OPEN_OPTIONS) : Collections.<Record>emptyIterator();

        Map<ByteBuffer, Record> records = new TreeMap<>();
        boolean isDeleting = record.getValue() == null;
        while (data.hasNext()) {
            var oldRecord = data.next();

            if (oldRecord.getKey().compareTo(record.getKey()) != 0) {
                records.put(oldRecord.getKey(), oldRecord);
            }
        }

        if (!isDeleting) {
            records.put(record.getKey(), record);
        }

        List<Iterator<Record>> recordsToMerge = new ArrayList<>(List.of(records.values().iterator()));
        Index nextIndex = indexes.get(index.order + 1);
        if (nextIndex != null) {
            recordsToMerge.addAll(getDataFromIndex(nextIndex));
        }

        flush(index, DAO.merge(recordsToMerge), WRITE_OPTIONS);
    }

    private List<Iterator<Record>> getDataFromIndex(Index index) throws IOException {
        List<Iterator<Record>> recordsToMerge = new ArrayList<>();
        Index nextIndex = indexes.get(index.order);

        while (nextIndex != null) {
            var path = getPath(getFileName(nextIndex.order));

            if (Files.exists(path)) {
                recordsToMerge.add(read(path, READ_OPEN_OPTIONS));
            }

            nextIndex = indexes.get(nextIndex.order + 1);
        }

        return recordsToMerge;
    }

    private void flush(Index index, Iterator<Record> data, final Set<? extends OpenOption> options) throws IOException {
        var currentIndex = indexes.putIfAbsent(index.order, new Index(index.order, null, 0));

        while (data.hasNext()) {
            var path = getPath(getFileName(currentIndex.order));
            Files.deleteIfExists(path);

            var inFile = 0;
            try (var ch = FileChannel.open(path, options)) {
                for (var i = 0; i < FILE_RECORD_LIMIT && data.hasNext(); i++) {
                    var record = data.next();

                    flush(ch, record.getKey());
                    flush(ch, record.getValue());

                    if (i == 0) {
                        currentIndex.startKey = record.getKey();
                    }

                    inFile++;
                }
            }

            currentIndex.recordAmount = inFile;

            if (!data.hasNext()) {
                return;
            }

            currentIndex = indexes.putIfAbsent(currentIndex.order + 1, new Index(currentIndex.order + 1, null, 0));
        }
    }

    private Iterator<Record> read(Path path, final Set<? extends OpenOption> options) throws IOException {
        NavigableMap<ByteBuffer, Record> tmpStore = new ConcurrentSkipListMap<>();
        try (var fc = FileChannel.open(path, options)) {
            var mappedBuffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());

            while (mappedBuffer.hasRemaining()) {
                var key = readByteBuffer(mappedBuffer);
                var value = readByteBuffer(mappedBuffer);

                tmpStore.put(key, Record.of(key, value));
            }
        }

        return tmpStore.values().iterator();
    }

    private ByteBuffer readByteBuffer(MappedByteBuffer mappedBuffer) {
        var size = mappedBuffer.getInt();
        var buffer = mappedBuffer.slice().limit(size).asReadOnlyBuffer();
        mappedBuffer.position(mappedBuffer.position() + size);

        return buffer;
    }

    private void flush(FileChannel fc, ByteBuffer buffer) throws IOException {
        fc.write(getBufferSize(buffer));
        fc.write(buffer);
    }

    private ByteBuffer getBufferSize(ByteBuffer buffer) {
        return ByteBuffer.wrap(toByteArray(buffer.remaining()));
    }

    private byte[] toByteArray(int n) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(n).array();
    }

    private String getFileName(int index) {
        return "file" + index + ".dat";
    }

    private Path getPath(String fileName) {
        return config.getDir().resolve(fileName);
    }
}

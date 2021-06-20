package ru.mail.polis.lsm.ponomarev;

import ru.mail.polis.lsm.DAO;
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

class Refrigerator {
    private static final int FILE_RECORD_LIMIT = 1;
    private static final String INDEXES_FILE_NAME = "index.info.dat";

    private static final Set<? extends OpenOption> READ_OPEN_OPTIONS = EnumSet.of(StandardOpenOption.READ);
    private static final Set<? extends OpenOption> WRITE_OPTIONS
            = EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE_NEW);
    private static final Comparator<Index> indexComparator = Comparator.comparing(i -> i.order);
    
    private final Path dir;
    private final Map<Integer, Index> indexes;

    private Index minIndex;
    private Index maxIndex;

    private static class Index {
        public final int order;
        public final ByteBuffer startKey;
        public final int recordAmount;

        public Index(int order, ByteBuffer startKey, int recordAmount) {
            this.order = order;
            this.startKey = startKey;
            this.recordAmount = recordAmount;
        }
    }

    /**
     * 
     * @param dir текущая директория
     * @throws IOException
     */
    public Refrigerator(Path dir) throws IOException {
        this.dir = dir;
        this.indexes = readIndexes();
    }

    public List<Iterator<Record>> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        try {
            if (this.indexes.isEmpty()) {
                return new ArrayList<>();
            }

            var index = fromKey == null ? minIndex : findIndex(fromKey);

            return (fromKey == null && toKey == null)
                    ? getAllData() :
                    new ArrayList<>(readData(index));

        } catch (IOException e) {
            throw new IllegalStateException("Something wrong", e);
        }
    }

    public void flush(NavigableMap<ByteBuffer, Record> store) throws IOException {
        for (var record : store.values()) {
            resolveMinMaxIndexes(record.getKey());
        }

        for (var record : store.values()) {
            flush(record);
        }

        writeIndexes(WRITE_OPTIONS);
    }

    private void resolveMinMaxIndexes(ByteBuffer key) {
        Index index = null;

        if (minIndex == null || maxIndex == null) {
            index = new Index(0, key, 0);

            maxIndex = index;
            minIndex = index;
        }

        var compareResult = key.compareTo(minIndex.startKey);
        if (compareResult < 0) {
            if (maxIndex.recordAmount == FILE_RECORD_LIMIT) {
                index = new Index(minIndex.order - 1, key, 0);
            } else {
                index = new Index(minIndex.order, key, minIndex.recordAmount);
            }

            minIndex = index;
        }

        if (compareResult > 0) {
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

    private void flush(Record record) throws IOException {
        var index = findIndex(record.getKey());
        var path = getPath(getFileName(index.order));
        var data = Files.exists(path) ? readRecords(path) : Collections.<Record>emptyIterator();

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
            recordsToMerge.addAll(readData(nextIndex));
        }

        moveData(index, DAO.merge(recordsToMerge));
    }

    private void moveData(Index index, Iterator<Record> data) throws IOException {
        var currentIndex
                = indexes.computeIfAbsent(index.order, c -> new Index(index.order, null, 0));

        if (!data.hasNext()) {
            Files.deleteIfExists(getPath(getFileName(currentIndex.order)));
        }

        while (data.hasNext()) {
            var path = getPath(getFileName(currentIndex.order));
            Files.deleteIfExists(path);

            var inFile = 0;
            Record firstRecord = null;
            try (var ch = FileChannel.open(path, WRITE_OPTIONS)) {
                for (var i = 0; i < FILE_RECORD_LIMIT && data.hasNext(); i++) {
                    var record = data.next();

                    writeRecord(ch, record.getKey());
                    writeRecord(ch, record.getValue());

                    if (firstRecord == null) {
                        firstRecord = record;
                    }

                    inFile++;
                }
            }

            if (firstRecord != null) {
                indexes.put(currentIndex.order, new Index(currentIndex.order, firstRecord.getKey(), inFile));
            }

            if (!data.hasNext()) {
                return;
            }

            var i = currentIndex.order + 1;
            currentIndex = indexes.computeIfAbsent(i, c -> new Index(i, null, 0));
        }
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

    private void writeRecord(FileChannel fc, ByteBuffer buffer) throws IOException {
        fc.write(getBufferSize(buffer));
        fc.write(buffer);
    }

    private ByteBuffer getBufferSize(ByteBuffer buffer) {
        return ByteBuffer.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(buffer.remaining()).array());
    }

    private List<Iterator<Record>> readData(Index index) throws IOException {
        List<Iterator<Record>> recordsToMerge = new ArrayList<>();
        Index nextIndex = indexes.get(index.order);

        while (nextIndex != null) {
            var path = getPath(getFileName(nextIndex.order));

            if (Files.exists(path)) {
                recordsToMerge.add(readRecords(path));
            }

            nextIndex = indexes.get(nextIndex.order + 1);
        }

        return recordsToMerge;
    }

    private Index findIndex(ByteBuffer key) {
        List<Index> indexList = new ArrayList<>(indexes.values());

        while (indexList.size() > 1) {
            var index = indexList.size() / 2;
            var midIndex = indexList.get(index);

            var compareResult = key.compareTo(midIndex.startKey);

            if (compareResult == 0) {
                return midIndex;
            }

            if (compareResult < 0) {
                indexList = indexList.subList(0, index);
            }

            if (compareResult > 0) {
                indexList = indexList.subList(index, indexList.size());
            }
        }

        return indexList.get(0);
    }

    private List<Iterator<Record>> getAllData() throws IOException {
        List<Iterator<Record>> iterators = new ArrayList<>();

        for (var i : indexes.values()) {
            var path = getPath(getFileName(i.order));
            if (Files.exists(path)) {
                var iterator = readRecords(path);
                iterators.add(iterator);
            }
        }

        return iterators;
    }

    private Map<Integer, Index> readIndexes() throws IOException {
        var path = getPath(INDEXES_FILE_NAME);

        if (Files.notExists(path)) {
            return new ConcurrentSkipListMap<>();
        }

        final Map<Integer, Index> indexesTmp = new ConcurrentSkipListMap<>();
        try (var fc = FileChannel.open(path, READ_OPEN_OPTIONS)) {
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

    private Iterator<Record> readRecords(Path path) throws IOException {
        NavigableMap<ByteBuffer, Record> tmpStore = new ConcurrentSkipListMap<>();
        try (var fc = FileChannel.open(path, READ_OPEN_OPTIONS)) {
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

    private Path getPath(String fileName) {
        return dir.resolve(fileName);
    }

    private String getFileName(int index) {
        return "file" + index + ".dat";
    }
}

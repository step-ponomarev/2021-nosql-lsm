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
import java.util.stream.Collectors;

class SSTable {
    private static final int FILE_RECORD_LIMIT = 1024;
    private static final String INDEXES_FILE_NAME = "index.info.dat";

    private static final Set<? extends OpenOption> READ_OPEN_OPTIONS = EnumSet.of(StandardOpenOption.READ);
    private static final Set<? extends OpenOption> WRITE_OPTIONS
            = EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE_NEW);
    private static final Comparator<Index> indexComparator = Comparator.comparing(i -> i.startKey);

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
     * @param dir текущая директория
     * @throws IOException
     */
    public SSTable(Path dir) throws IOException {
        this.dir = dir;
        this.indexes = readIndexes();
    }

    public List<Iterator<Record>> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        try {
            if (this.indexes.isEmpty()) {
                return new ArrayList<>();
            }

            if (fromKey == null && toKey == null) {
                return getAllData();
            }

            var fromIndex = fromKey == null ? minIndex : findIndex(fromKey);
            var toIndex = toKey == null ? maxIndex : findIndex(toKey);

            return readData(fromIndex, toIndex);
        } catch (IOException e) {
            throw new IllegalStateException("Something wrong", e);
        }
    }

    public void flush(NavigableMap<ByteBuffer, Record> store) throws IOException {
        for (var record : store.values()) {
            resolveIndex(record.getKey());
            flush(record);
        }

        writeIndexes(WRITE_OPTIONS);
    }

    public void resolveIndex(ByteBuffer key) {
        Index index = null;

        if (minIndex == null || maxIndex == null) {
            index = new Index(0, key, 0);
        }

        if (minIndex != null && key.compareTo(minIndex.startKey) < 0 && minIndex.recordAmount == FILE_RECORD_LIMIT) {
            index = new Index(minIndex.order - 1, key, 0);
        }

        if (maxIndex != null && key.compareTo(maxIndex.startKey) > 0 && maxIndex.recordAmount == FILE_RECORD_LIMIT) {
            index = new Index(maxIndex.order + 1, key, 0);
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
        if (nextIndex != null && maxIndex != null) {
            recordsToMerge.addAll(readData(nextIndex, maxIndex));
        }

        moveData(index, DAO.merge(recordsToMerge));
    }

    private void moveData(Index index, Iterator<Record> data) throws IOException {
        var currentOrder = index.order;

        if (!data.hasNext()) {
            Files.deleteIfExists(getPath(getFileName(currentOrder)));
            this.indexes.remove(index.order);
        }

        while (data.hasNext()) {
            var path = getPath(getFileName(currentOrder));
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
                indexes.put(currentOrder, new Index(currentOrder, firstRecord.getKey(), inFile));
            }

            currentOrder++;
        }

        resolveMaxMin();
    }

    private void resolveMaxMin() {
        minIndex = this.indexes.values().stream().min(Comparator.comparing(l -> l.startKey)).orElse(null);
        maxIndex = this.indexes.values().stream().max(Comparator.comparing(l -> l.startKey)).orElse(null);
    }

    private void writeIndexes(final Set<? extends OpenOption> options) throws IOException {
        var path = getPath(INDEXES_FILE_NAME);
        Files.deleteIfExists(path);

        try (var fc = FileChannel.open(path, options)) {
            for (var index : indexes.values()) {
                fc.write(toByteBuffer(index.order));
                writeRecord(fc, index.startKey);
                fc.write(toByteBuffer(index.recordAmount));
            }
        }
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

    private void writeRecord(FileChannel fc, ByteBuffer buffer) throws IOException {
        fc.write(toByteBuffer(buffer.remaining()));
        fc.write(buffer);
    }

    private ByteBuffer toByteBuffer(int n) {
        return ByteBuffer.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(n).array());
    }

    private List<Iterator<Record>> readData(Index fromIndex, Index toIndex) throws IOException {
        List<Iterator<Record>> recordsToMerge = new ArrayList<>();
        var indexes = this.indexes.values()
                .stream()
                .filter(f -> filterBetween(f.startKey, fromIndex.startKey, toIndex.startKey))
                .sorted(indexComparator)
                .collect(Collectors.toList());

        for (var index : indexes) {
            var path = getPath(getFileName(index.order));

            if (Files.exists(path)) {
                recordsToMerge.add(readRecords(path));
            }
        }

        return recordsToMerge;
    }

    private boolean filterBetween(ByteBuffer currentKey, ByteBuffer startKey, ByteBuffer endKey) {
        if (currentKey.compareTo(startKey) < 0) {
            return false;
        }

        return currentKey.compareTo(endKey) <= 0;
    }

    private Index findIndex(ByteBuffer key) {
        List<Index> indexList = indexes.values()
                .stream()
                .sorted(indexComparator)
                .collect(Collectors.toList());

        while (indexList.size() > 1) {
            var index = indexList.size() / 2;
            var midIndex = indexList.get(index);

            var compareResult = key.compareTo(midIndex.startKey);

            if (compareResult == 0) {
                return midIndex;
            }

            if (compareResult <= 0) {
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

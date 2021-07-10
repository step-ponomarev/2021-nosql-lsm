package ru.mail.polis.lsm.ponomarev;

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
    private static final class Index {
        private final ByteBuffer key;
        private final int position;

        public Index(ByteBuffer key, int position) {
            this.key = key;
            this.position = position;
        }
    }

    private static final String RECORD_FILE_POSTFIX = ".rec";
    private static final String INDEX_FILE_POSTFIX = ".index";
    
    private static final int FILE_SIZE_LIMIT = Integer.MAX_VALUE - 1024;

    private static final Set<? extends OpenOption> COMMON_READ_OPEN_OPTIONS = EnumSet.of(StandardOpenOption.READ);
    private static final Set<? extends OpenOption> APPEND_WRITE_OPTION
            = EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.APPEND);

    private static final Set<? extends OpenOption> CREATE_NEW_WRITE_OPTION
            = EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE);

    private final Path dir;

    public SSTable(Path dir) {
        this.dir = dir;
    }

    /**
     * Сохраняет данные на диск.
     *
     * @param records данные.
     * @throws IOException выбрасывает в случае ошибки записи.
     */
    public synchronized void flush(Iterator<Record> records) throws IOException {
        Path recordFile = getPath(RECORD_FILE_POSTFIX);

        if (Files.notExists(recordFile)) {
            Files.createFile(recordFile);
        }

        long fileSize = Files.size(recordFile);
        if (fileSize >= FILE_SIZE_LIMIT) {
            var recordIterator = read(null, null);

            try (var fileChannel = FileChannel.open(recordFile, CREATE_NEW_WRITE_OPTION)) {
                writeRecords(recordIterator, fileChannel);
            }
        }

        try (var fileChannel = FileChannel.open(recordFile, APPEND_WRITE_OPTION)) {
            writeRecords(records, fileChannel);
        }
    }

    private void writeRecords(Iterator<Record> records, FileChannel fileChannel) throws IOException {
        final NavigableMap<ByteBuffer, Index> indices = new ConcurrentSkipListMap<>();
        while (records.hasNext()) {
            Record record = records.next();

            // Смещение ByteBuffer не превышает Integer.MAX_VALUE.
            int positionOfCurrentKey = (int) fileChannel.position();
            writeRecord(fileChannel, record);

            ByteBuffer key = record.getKey();
            indices.put(key, new Index(key, positionOfCurrentKey));
        }

        writeIndices(indices, APPEND_WRITE_OPTION);
    }

    public synchronized Iterator<Record> read(ByteBuffer fromKey, ByteBuffer toKey) throws IOException {
        final Path recordFile = getPath(RECORD_FILE_POSTFIX);

        if (Files.notExists(recordFile)) {
            return Collections.emptyIterator();
        }

        final Map<ByteBuffer, Record> records = new ConcurrentSkipListMap<>();
        try (var fileChannel = FileChannel.open(recordFile, COMMON_READ_OPEN_OPTIONS)) {
            var mappedByteBuffer = fileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    fileChannel.size()
            );

            NavigableMap<ByteBuffer, Index> allIndices = readIndices();
            List<Index> indexes = filterIndices(allIndices.values(), fromKey, toKey);
            for (var index : indexes) {
                mappedByteBuffer.position(index.position);
                Record record = readRecord(mappedByteBuffer);

                if (record.isTombstone()) {
                    records.remove(record.getKey());
                } else {
                    records.put(record.getKey(), record);
                }
            }

            writeIndices(allIndices, CREATE_NEW_WRITE_OPTION);
        }

        return records.values().iterator();
    }

    /**
     * @param indices предоставленные индексы.
     * @param fromKey ключ, меньше которого мы отсеиваем.
     * @param toKey   ключ, больше которого мы отсеиваем.
     * @return
     */
    private List<Index> filterIndices(Collection<Index> indices, @Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        return indices
                .stream()
                .filter(i -> filterIndex(i, fromKey, toKey))
                .sorted(Comparator.comparing(l -> l.key))
                .collect(Collectors.toList());
    }

    private boolean filterIndex(Index i, ByteBuffer fromKey, ByteBuffer toKey) {
        ByteBuffer key = i.key;

        if (fromKey == null && toKey == null) {
            return true;
        }

        if (fromKey == null) {
            return key.compareTo(toKey) <= 0;
        }

        if (toKey == null) {
            return key.compareTo(fromKey) >= 0;
        }

        return key.compareTo(fromKey) >= 0 && key.compareTo(toKey) <= 0;
    }

    /**
     * Сохраняем индексы с учетом новых.
     *
     * @throws IOException в случае ошибки записи.
     */
    private void writeIndices(NavigableMap<ByteBuffer, Index> indices, Set<? extends OpenOption> writeOptions) throws IOException {
        Path indexFile = getPath(INDEX_FILE_POSTFIX);

        if (Files.notExists(indexFile)) {
            Files.createFile(indexFile);
        }

        try (var fileChannel = FileChannel.open(indexFile, writeOptions)) {
            for (var index : indices.values()) {
                writeIndex(fileChannel, index);
            }
        }
    }

    /**
     * Записываем индеус на диск.
     *
     * @param fileChannel канал через который будем записывать.
     * @param index       индекс, который будет сохранен на диск.
     * @throws IOException выбрасывает в случае ошибки записи.
     */
    private void writeIndex(FileChannel fileChannel, Index index) throws IOException {
        writeByteBufferWithSize(fileChannel, index.key);
        fileChannel.write(convertToByteBuffer(index.position));
    }

    /**
     * Читает индексы с диска.
     *
     * @return Возвращает мапу индексов.
     * @throws IOException в случае ошибки чтения.
     */
    private NavigableMap<ByteBuffer, Index> readIndices() throws IOException {
        final Path indexesFile = getPath(INDEX_FILE_POSTFIX);

        if (Files.notExists(indexesFile)) {
            return new ConcurrentSkipListMap<>();
        }

        Map<ByteBuffer, Index> indexes = new TreeMap<>();
        try (var fileChannel = FileChannel.open(indexesFile, COMMON_READ_OPEN_OPTIONS)) {
            var mappedByteBuffer = fileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    fileChannel.size()
            );

            while (mappedByteBuffer.hasRemaining()) {
                Index index = readIndex(mappedByteBuffer);
                indexes.put(index.key, index);
            }
        }

        return new ConcurrentSkipListMap<>(indexes);
    }

    /**
     * Читает индекс записи.
     *
     * @param mappedByteBuffer через него осуществляется чтение.
     * @return Возвращает индекс записи.
     */
    private Index readIndex(MappedByteBuffer mappedByteBuffer) {
        ByteBuffer key = readByteBufferWithSize(mappedByteBuffer);
        int position = mappedByteBuffer.getInt();

        return new Index(key, position);
    }

    /**
     * @param fileChannel канал через который будем записывать.
     * @param record      запись, которую сохраняем на диск.
     * @throws IOException выбрасывает в случае ошибки записи.
     */
    private void writeRecord(FileChannel fileChannel, Record record) throws IOException {
        ByteBuffer key = record.getKey();
        ByteBuffer value = record.getValue();

        writeByteBufferWithSize(fileChannel, key);
        writeByteBufferWithSize(fileChannel, value);
    }

    /**
     * Читает запись с диска.
     *
     * @param mappedByteBuffer через него осуществляется чтение.
     * @return Запись, прочитанная с диска.
     */
    private Record readRecord(MappedByteBuffer mappedByteBuffer) {
        ByteBuffer key = readByteBufferWithSize(mappedByteBuffer);
        ByteBuffer value = readByteBufferWithSize(mappedByteBuffer);

        if (key == null) {
            throw new IllegalStateException("Key mustn't be null");
        }

        if (value == null) {
            return Record.tombstone(key);
        }

        return Record.of(key, value);
    }

    /**
     * Метод сохраняет на диск размер записи, а затем запись.
     * Если запись null пишем отрицательный размер на диск.
     *
     * @param fileChannel канал через который будем записывать.
     * @param buffer      запись, которую сохраняем на диск.
     * @throws IOException выбрасывает в случае ошибки записи.
     */
    private void writeByteBufferWithSize(FileChannel fileChannel, @Nullable ByteBuffer buffer) throws IOException {
        if (buffer == null) {
            fileChannel.write(convertToByteBuffer(-1));
            return;
        }

        int size = buffer.remaining();
        fileChannel.write(convertToByteBuffer(size));
        fileChannel.write(buffer.asReadOnlyBuffer());
    }

    /**
     * Читает ByteBuffer.
     *
     * @param mappedByteBuffer через него осуществляется чтение.
     * @return возвращает запись, может вернуть null.
     */
    @Nullable
    private ByteBuffer readByteBufferWithSize(MappedByteBuffer mappedByteBuffer) {
        int size = mappedByteBuffer.getInt();
        if (size < 0) {
            return null;
        }

        ByteBuffer buffer = mappedByteBuffer.slice().limit(size).asReadOnlyBuffer();
        mappedByteBuffer.position(mappedByteBuffer.position() + size);

        return buffer;
    }

    private ByteBuffer convertToByteBuffer(int n) {
        return ByteBuffer.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(n).array());
    }

    private Path getPath(String postfix) {
        return dir.resolve("file" + postfix);
    }
}

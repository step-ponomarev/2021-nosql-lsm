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

class SSTable {
    private static final String RECORD_FILE_POSTFIX = ".rec";
    private static final String INDEX_FILE_POSTFIX = ".ind";

    private static final Set<? extends OpenOption> READ_OPEN_OPTIONS = EnumSet.of(StandardOpenOption.READ);
    private static final Set<? extends OpenOption> WRITE_OPTIONS
            = EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.APPEND);

    private final Path dir;

    public SSTable(Path dir) {
        this.dir = dir;
    }

    public synchronized Iterator<Record> read(ByteBuffer fromKey, ByteBuffer toKey) throws IOException {
        final Map<ByteBuffer, Record> records = new ConcurrentSkipListMap<>();
        final Path recordFile = getPath(RECORD_FILE_POSTFIX);
        
        if (Files.notExists(recordFile)) {
            return Collections.emptyIterator();
        }

        try (var recordFileChannel = FileChannel.open(recordFile, READ_OPEN_OPTIONS)) {
            var mappedByteBuffer = recordFileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    recordFileChannel.size()
            );

            while (mappedByteBuffer.hasRemaining()) {
                Record record = readRecord(mappedByteBuffer);
                if (record.isTombstone()) {
                    records.remove(record.getKey());
                } else {
                    records.put(record.getKey(), record);
                }
            }
        }

        if (fromKey == null && toKey == null) {
            return records.values().iterator();
        }

        return records.values()
                .stream()
                .filter(r -> filterRecords(r, fromKey, toKey))
                .iterator();
    }

    private boolean filterRecords(Record record, ByteBuffer fromKey, ByteBuffer toKey) {
        if (fromKey == null) {
            return record.getKey().compareTo(toKey) <= 0;
        }

        if (toKey == null) {
            return record.getKey().compareTo(fromKey) >= 0;
        }

        return record.getKey().compareTo(fromKey) >= 0
                && record.getKey().compareTo(toKey) <= 0;
    }

    /**
     * Читает запись с диска.
     *
     * @param mappedByteBuffer через него осуществляется чтение.
     * @return Запись, прочитанная с диска.
     */
    private Record readRecord(MappedByteBuffer mappedByteBuffer) {
        ByteBuffer key = readByteBuffer(mappedByteBuffer);
        ByteBuffer value = readByteBuffer(mappedByteBuffer);

        if (key == null) {
            throw new IllegalStateException("Key mustn't be null");
        }

        if (value == null) {
            return Record.tombstone(key);
        }

        return Record.of(key, value);
    }

    /**
     * Читает ByteBuffer.
     *
     * @param mappedByteBuffer через него осуществляется чтение.
     * @return возвращает запись, может вернуть null.
     */
    @Nullable
    private ByteBuffer readByteBuffer(MappedByteBuffer mappedByteBuffer) {
        int size = mappedByteBuffer.getInt();
        if (size < 0) {
            return null;
        }

        ByteBuffer buffer = mappedByteBuffer.slice().limit(size).asReadOnlyBuffer();
        mappedByteBuffer.position(mappedByteBuffer.position() + size);

        return buffer;
    }

    //TODO: Нужно как-то читать индексы( их нужно держать в актуальном состоянии ).  

    /**
     * Сохраняет данные на диск.
     *
     * @param records данные.
     * @throws IOException выбрасывает в случае ошибки записи.
     */
    public synchronized void flush(Iterable<Record> records) throws IOException {
        Path indexFile = getPath(INDEX_FILE_POSTFIX);
        Path recordFile = getPath(RECORD_FILE_POSTFIX);

        if (Files.notExists(indexFile)) {
            Files.createFile(indexFile);
        }
        
        if (Files.notExists(recordFile)) {
            Files.createFile(recordFile);
        }

        try (var indexFileChannel = FileChannel.open(indexFile, WRITE_OPTIONS);
             var recordFileChannel = FileChannel.open(recordFile, WRITE_OPTIONS);
        ) {
            for (Record record : records) {
                // Смещение ByteBuffer не превышает Integer.MAX_VALUE.
                int positionOfCurrentKey = (int) recordFileChannel.position();
                writeIndex(indexFileChannel, record.getKey(), positionOfCurrentKey);
                writeRecord(recordFileChannel, record);
            }
        }
    }

    /**
     * @param fileChannel канал через который будем записывать.
     * @param key         ключ, для которого мы сохраняем индекс.
     * @param position    позиция для текущего ключа.
     * @throws IOException выбрасывает в случае ошибки записи.
     */
    private void writeIndex(FileChannel fileChannel, ByteBuffer key, int position) throws IOException {
        writeByteBuffer(fileChannel, key);
        writeByteBuffer(fileChannel, convertToByteBuffer(position));
    }

    /**
     * @param fileChannel канал через который будем записывать.
     * @param record      запись, которую сохраняем на диск.
     * @throws IOException выбрасывает в случае ошибки записи.
     */
    private void writeRecord(FileChannel fileChannel, Record record) throws IOException {
        ByteBuffer key = record.getKey();
        ByteBuffer value = record.getValue();

        writeByteBuffer(fileChannel, key);
        writeByteBuffer(fileChannel, value);
    }

    /**
     * Метод сохраняет на диск размер записи, а затем запись.
     * Если запись null пишем отрицательный размер на диск.
     *
     * @param fileChannel канал через который будем записывать.
     * @param buffer      запись, которую сохраняем на диск.
     * @throws IOException выбрасывает в случае ошибки записи.
     */
    private void writeByteBuffer(FileChannel fileChannel, @Nullable ByteBuffer buffer) throws IOException {
        if (buffer == null) {
            fileChannel.write(convertToByteBuffer(-1));
            return;
        }

        int size = buffer.remaining();
        fileChannel.write(convertToByteBuffer(size));
        fileChannel.write(buffer);
    }

    private ByteBuffer convertToByteBuffer(int n) {
        return ByteBuffer.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(n).array());
    }

    private Path getPath(String postfix) {
        return dir.resolve("file" + postfix);
    }
}

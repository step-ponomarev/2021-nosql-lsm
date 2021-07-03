package ru.mail.polis.lsm.ponomarev;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Set;

class SSTable {
    private static final String DATA_FILE_POSTFIX = ".dat";
    private static final String INDEX_FILE_POSTFIX = ".ind";

    private static final Set<? extends OpenOption> READ_OPEN_OPTIONS = EnumSet.of(StandardOpenOption.READ);
    private static final Set<? extends OpenOption> WRITE_OPTIONS
            = EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE_NEW);

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
    public void flush(Iterable<Record> records) throws IOException {
        try (var indexFileChannel = FileChannel.open(dir.resolveSibling(INDEX_FILE_POSTFIX), WRITE_OPTIONS);
             var recordsFileChannel = FileChannel.open(dir.resolveSibling(DATA_FILE_POSTFIX), WRITE_OPTIONS);
        ) {
            for (Record record : records) {
                // Смещение ByteBuffer не превышает Integer.MAX_VALUE.
                int positionOfCurrentKey = (int) recordsFileChannel.position();
                writeIndex(indexFileChannel, record.getKey(), positionOfCurrentKey);
                writeRecord(recordsFileChannel, record);
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
     * @return Возвращает позицию после записи.
     * @throws IOException выбрасывает в случае ошибки записи.
     */
    private int writeRecord(FileChannel fileChannel, Record record) throws IOException {
        ByteBuffer key = record.getKey();
        ByteBuffer value = record.getValue();

        writeByteBuffer(fileChannel, key);
        writeByteBuffer(fileChannel, value);

        return fileChannel.write(convertToByteBuffer(key.remaining()));
    }

    /**
     * Метод сохраняет на диск размер записи, а затем запись.
     * Если запись null пишем отрицательный размер на диск.
     *
     * @param fileChannel канал через который будем записывать.
     * @param buffer      запись, которую сохраняем на диск.
     * @return Возвращает позицию после записи.
     * @throws IOException выбрасывает в случае ошибки записи.
     */
    private int writeByteBuffer(FileChannel fileChannel, @Nullable ByteBuffer buffer) throws IOException {
        if (buffer == null) {
            return fileChannel.write(convertToByteBuffer(-1));
        }

        int size = buffer.remaining();
        fileChannel.write(convertToByteBuffer(size));

        return fileChannel.write(buffer);
    }

    private ByteBuffer convertToByteBuffer(int n) {
        return ByteBuffer.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(n).array());
    }
}

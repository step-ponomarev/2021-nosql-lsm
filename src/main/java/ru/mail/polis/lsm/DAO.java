package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Minimal database API.
 */
public interface DAO extends Closeable {
    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);

    void upsert(Record record);

    /**
     * Appends {@code Byte.MIN_VALUE} to {@code buffer}.
     *
     * @param buffer original {@link ByteBuffer}
     * @return copy of {@code buffer} with {@code Byte.MIN_VALUE} appended
     */
    static ByteBuffer nextKey(ByteBuffer buffer) {
        ByteBuffer result = ByteBuffer.allocate(buffer.remaining() + 1);

        int position = buffer.position();

        result.put(buffer);
        result.put(Byte.MIN_VALUE);

        buffer.position(position);
        result.rewind();

        return result;
    }

    static Iterator<Record> merge(List<Iterator<Record>> iterators) {        
        return iterators.stream()
                .filter(Iterator::hasNext)
                .flatMap(DAO::toStream)
                .collect(Collectors.toMap(Record::getKey, UnaryOperator.identity(), (v1, v2) -> v2))
                .values()
                .stream()
                .sorted(Comparator.comparing(Record::getKey))
                .iterator();
    }

    private static Stream<Record> toStream(Iterator<Record> iterator) {
        var set = new HashSet<Record>();

        while (iterator.hasNext()) {
            set.add(iterator.next());
        }

        return set.stream();
    }
}

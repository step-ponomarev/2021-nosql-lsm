package ru.mail.polis.lsm;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Minimal database API.
 */
public interface DAO extends Closeable {
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

    static int ДЕРЖИМ_ВОТ_СТОЛЬКО_НАГРУЗКИ = 1000000;
    /**
     * Метод сливает итераторы в один, упорядочивая по возрастанию, 
     * если данные повторяются - берет последнюю версию данных.
     * 
     * @param iterators список итераторов для слияния
     * @return последовательность итераторов
     */
    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        return iterators.stream()
                .flatMap(DAO::toStream)
                .limit(ДЕРЖИМ_ВОТ_СТОЛЬКО_НАГРУЗКИ)
                .collect(Collectors.groupingBy(Record::getKey))
                .values()
                .stream()
                .reduce(new ArrayList<>(), 
                        ((l, r) -> {
                            var rList = r.stream()
                                    .filter(rec -> Objects.equals(rec, r.get(r.size() - 1)))
                                    .collect(Collectors.toList());
                            
                            l.addAll(rList);
                            
                            return l;
                        }))
                .stream().sorted(Comparator.comparing(Record::getKey)).iterator();
    }

    private static Stream<Record> toStream(Iterator<Record> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);

    void upsert(Record record);
}

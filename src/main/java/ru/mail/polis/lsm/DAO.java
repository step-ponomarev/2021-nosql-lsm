package ru.mail.polis.lsm;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;

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

    /**
     * <p>
     * Метод сливает итераторы в один, упорядочивая по возрастанию,
     * если данные повторяются - берет последнюю версию данных.
     * </p>
     *
     * <p>
     * Будет обработано не более <tt>1_000_000</tt>
     * элементов каждого итератора.
     * </p>
     *
     * @param iterators список итераторов для слияния
     * @return последовательность итераторов
     */
    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        return MergeIterator.instanceOf(iterators);
    }

    class MergeIterator implements Iterator<Record> {
        public static Iterator<Record> instanceOf(final List<Iterator<Record>> iterators) {
            if (iterators.isEmpty()) {
                return Collections.emptyIterator();
            }
            
            if (iterators.size() == 1) {
                return iterators.get(0);
            }
            
            Iterator<Record> left = instanceOf(iterators.subList(0, iterators.size() / 2));
            Iterator<Record> right = instanceOf(iterators.subList(iterators.size() / 2, iterators.size()));
            
            return merge(left, right);
        }
        
        private static Iterator<Record> merge(Iterator<Record> left, Iterator<Record> right) {
            return new MergeIterator(left, right);
        }
        
        private final Iterator<Record> firstIter;
        private final Iterator<Record> secondIter;

        private Record elem1;
        private Record elem2;

        private MergeIterator(Iterator<Record> firstIter, Iterator<Record> secondIter) {
            this.firstIter = firstIter;
            this.secondIter = secondIter;
            
            elem1 = getElement(firstIter);
            elem2 = getElement(secondIter);
        }

        @Override
        public Record next() {
//            if (!hasNext()) {
//                throw new NullPointerException("No Such Element");
//            }

            var compareResult = compare(elem1, elem2);
            var next = compareResult > 0
                    ? elem2
                    : elem1;

            if (compareResult < 0) {
                elem1 = getElement(firstIter);
            }

            if (compareResult > 0) {
                elem2 = getElement(secondIter);
            }

            if (compareResult == 0) {
                elem1 = getElement(firstIter);
                elem2 = getElement(secondIter);
            }

            return next;
        }

        @Override
        public boolean hasNext() {
            return elem1 != null || elem2 != null;
        }

        private int compare(@Nullable Record r1, @Nullable Record r2) {
            boolean firstNull = r1 == null;
            boolean secondNull = r2 == null;

            if (firstNull) {
                return 1;
            }

            if (secondNull) {
                return -1;
            }

            return r1.getKey().compareTo(r2.getKey());
        }

        private Record getElement(@Nonnull final Iterator<Record> iter) {
            return iter.hasNext() ? iter.next() : null;
        }
    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);

    void upsert(Record record);
}

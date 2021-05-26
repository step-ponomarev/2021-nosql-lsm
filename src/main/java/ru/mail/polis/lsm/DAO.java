package ru.mail.polis.lsm;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

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
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }
        
        return new MergeIterator(iterators);
    }

    class MergeIterator implements Iterator<Record> {
        private final List<Iterator<Record>> iterators;
        private Iterator<Record> currentIter;
        private int index = 0;

        private MergeIterator(@Nonnull List<Iterator<Record>> iteratorList) {
            this.iterators = new LinkedList<>(iteratorList);
            this.currentIter = iteratorList.get(index);
        }

        @Override
        public boolean hasNext() {
            while (iterators.size() > index && !this.iterators.get(index).hasNext()) {
                index++;
            }

            this.currentIter = this.iterators.get(index);

            return this.currentIter.hasNext();
        }

        @Override
        public Record next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more elements");
            }

            return currentIter.next();
        }
    }

}

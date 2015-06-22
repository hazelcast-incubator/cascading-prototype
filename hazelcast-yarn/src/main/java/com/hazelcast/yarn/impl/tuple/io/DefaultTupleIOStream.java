package com.hazelcast.yarn.impl.tuple.io;

import java.util.Arrays;
import java.util.Iterator;

import com.hazelcast.yarn.api.tuple.Tuple;
import com.hazelcast.yarn.api.tuple.TupleBufferAware;
import com.hazelcast.yarn.api.tuple.io.TupleInputStream;
import com.hazelcast.yarn.api.tuple.io.TupleOutputStream;

public class DefaultTupleIOStream
        implements TupleInputStream, TupleOutputStream<Object, Object>, TupleBufferAware {
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private int size = 0;
    private Tuple[] buffer;
    private int currentIdx = 0;
    private final Iterator<Tuple> iterator = new TupleIterator();

    public DefaultTupleIOStream(Tuple[] buffer) {
        this.buffer = buffer;
    }

    @Override
    public Tuple get(int idx) {
        return idx < buffer.length ? buffer[idx] : null;
    }

    @Override
    public Iterator<Tuple> iterator() {
        this.currentIdx = 0;
        return this.iterator;
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public Tuple<Object, Object> produce() {
        return null;
    }

    @Override
    public boolean consume(Tuple tuple) throws Exception {
        if (this.buffer == null) {
            this.buffer = new Tuple[1];
        }

        if (this.size >= this.buffer.length) {
            this.expand(this.size + 1);
        }

        this.buffer[this.size++] = tuple;
        return true;
    }

    @Override
    public void consumeChunk(Tuple[] chunk, int actualSize) {
        if (this.buffer.length < actualSize) {
            this.expand(actualSize);
        }

        System.arraycopy(chunk, 0, this.buffer, 0, actualSize);
        this.size = actualSize;
    }

    @Override
    public void consumeStream(TupleInputStream<Object, Object> inputStream) throws Exception {
        consumeChunk(((TupleBufferAware) inputStream).getBuffer(), inputStream.size());
    }

    @Override
    public Tuple[] getBuffer() {
        return this.buffer;
    }

    public void reset() {
        if (buffer != null) {
            Arrays.fill(buffer, null);
        }

        this.size = 0;
    }

    private static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) { // overflow
            throw new OutOfMemoryError();
        }

        return (minCapacity > MAX_ARRAY_SIZE) ?
                Integer.MAX_VALUE :
                MAX_ARRAY_SIZE;
    }

    private void expand(int minCapacity) {
        // overflow-conscious code
        int oldCapacity = this.buffer.length;
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity;
        if (newCapacity - MAX_ARRAY_SIZE > 0)
            newCapacity = hugeCapacity(minCapacity);
        // minCapacity is usually close to size, so this is a win:
        this.buffer = Arrays.copyOf(this.buffer, newCapacity);
    }

    public class TupleIterator implements Iterator<Tuple> {
        @Override
        public boolean hasNext() {
            return currentIdx < size();
        }

        @Override
        public Tuple next() {
            return get(currentIdx++);
        }
    }
}

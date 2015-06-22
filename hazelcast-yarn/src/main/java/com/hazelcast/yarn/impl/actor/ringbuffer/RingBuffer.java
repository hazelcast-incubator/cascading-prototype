package com.hazelcast.yarn.impl.actor.ringbuffer;


import sun.misc.Unsafe;

import java.util.Arrays;

import com.hazelcast.logging.ILogger;
import com.hazelcast.yarn.impl.YarnUtil;
import com.hazelcast.yarn.api.tuple.Tuple;

import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.yarn.api.tuple.TupleBufferAware;
import com.hazelcast.yarn.api.tuple.io.ProducerInputStream;


abstract class RingBufferPad {
    protected long p1, p2, p3, p4, p5, p6, p7;
}

abstract class RingBufferFields<T> extends RingBufferPad {
    protected static final int BUFFER_PAD;
    private static final Unsafe UNSAFE = YarnUtil.getUnsafe();

    static {
        final int scale = UNSAFE.arrayIndexScale(Object[].class);
        BUFFER_PAD = 128 / scale;
    }

    protected final long indexMask;
    protected final Object[] entries;
    protected final int bufferSize;

    RingBufferFields(
            int bufferSize
    ) {
        this.bufferSize = bufferSize;

        if (bufferSize < 1) {
            throw new IllegalArgumentException("bufferSize must not be less than 1");
        }
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }

        this.indexMask = bufferSize - 1;
        this.entries = new Object[bufferSize + 2 * BUFFER_PAD];
    }
}

public final class RingBuffer<T> extends RingBufferFields<T> {
    public static final long INITIAL_CURSOR_VALUE = 0L;

    protected long p1, p2, p3, p4, p5, p6, p7;

    private final PaddedLong readSequencer = new PaddedLong(RingBuffer.INITIAL_CURSOR_VALUE);

    private final PaddedLong writeSequencer = new PaddedLong(RingBuffer.INITIAL_CURSOR_VALUE);

    private final PaddedLong availableSequencer = new PaddedLong(RingBuffer.INITIAL_CURSOR_VALUE);

    private final ILogger logger;

    public RingBuffer(
            int bufferSize,
            ILogger logger
    ) {
        super(bufferSize);

        this.logger = logger;
    }

    public int acquire(int acquired) {
        if (acquired > this.bufferSize) {
            acquired = bufferSize;
        }

        int remaining = this.bufferSize - (int) (this.availableSequencer.getValue() - this.readSequencer.getValue());

        if (remaining <= 0) {
            return 0;
        }

        int realAcquired = Math.min(remaining, acquired);

        this.writeSequencer.setValue(this.availableSequencer.getValue() + realAcquired);

        return realAcquired;
    }

    public void commit(ProducerInputStream<T> chunk, int consumed) {
        long writerSequencerValue = this.writeSequencer.getValue();
        long availableSequencerValue = this.availableSequencer.getValue();

        int entriesStart = (int) (BUFFER_PAD + ((availableSequencerValue & indexMask)));
        int count = (int) (writerSequencerValue - availableSequencerValue);
        int window = entries.length - BUFFER_PAD - entriesStart;

        Tuple[] buffer = ((TupleBufferAware) chunk).getBuffer();

        RBCounterPut.addAndGet(count);

        if (count <= window) {
            System.arraycopy(
                    buffer,
                    consumed,
                    entries,
                    entriesStart,
                    count
            );
        } else {
            System.arraycopy(
                    buffer,
                    consumed,
                    entries,
                    entriesStart,
                    window
            );

            System.arraycopy(
                    buffer,
                    consumed + window,
                    entries,
                    BUFFER_PAD,
                    count - window
            );
        }

        this.availableSequencer.setValue(writerSequencerValue);
    }

    public static final AtomicInteger RBCounterFetch = new AtomicInteger(0);
    public static final AtomicInteger RBCounterPut = new AtomicInteger(0);


    public int fetch(T[] chunk) {
        long availableSequence = this.availableSequencer.getValue();
        long readerSequencerValue = this.readSequencer.getValue();

        int count = Math.min(chunk.length, (int) (availableSequence - readerSequencerValue));
        int entriesStart = (int) (BUFFER_PAD + ((readerSequencerValue & this.indexMask)));
        int window = this.entries.length - BUFFER_PAD - entriesStart;

        RBCounterFetch.addAndGet(count);

        if (count <= window) {
            System.arraycopy(this.entries, entriesStart, chunk, 0, count);
            Arrays.fill(this.entries, entriesStart, entriesStart + count, null);
        } else {
            System.arraycopy(this.entries, entriesStart, chunk, 0, window);
            Arrays.fill(this.entries, entriesStart, entriesStart + window, null);
            System.arraycopy(this.entries, BUFFER_PAD, chunk, window, count - window);
            Arrays.fill(this.entries, BUFFER_PAD, BUFFER_PAD + count - window, null);
        }

        this.readSequencer.setValue(this.readSequencer.getValue() + count);
        return count;
    }
}

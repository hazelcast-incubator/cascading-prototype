/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.actor.ringbuffer;

import sun.misc.Unsafe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.jet.api.actor.RingBuffer;
import com.hazelcast.jet.api.data.BufferAware;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.api.strategy.DataTransferringStrategy;

abstract class RingBufferPadByValue {
    protected long p1, p2, p3, p4, p5, p6, p7;
}

abstract class RingBufferFieldsByValue<T> extends RingBufferPadByValue {
    protected static final int BUFFER_PAD;
    private static final Unsafe UNSAFE = UnsafeHelper.UNSAFE;

    static {
        final int scale = UNSAFE.arrayIndexScale(Object[].class);
        BUFFER_PAD = 128 / scale;
    }

    protected final long indexMask;
    protected final T[] entries;
    protected final int bufferSize;
    protected final DataTransferringStrategy<T> dataTransferringStrategy;

    RingBufferFieldsByValue(
            int bufferSize,
            DataTransferringStrategy<T> dataTransferringStrategy
    ) {
        this.bufferSize = bufferSize;

        if (bufferSize < 1) {
            throw new IllegalArgumentException("bufferSize must not be less than 1");
        }

        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }

        this.dataTransferringStrategy = dataTransferringStrategy;

        this.indexMask = bufferSize - 1;
        this.entries = (T[]) new Object[bufferSize + 2 * BUFFER_PAD];

        for (int i = 0; i < this.entries.length; i++) {
            this.entries[i] = dataTransferringStrategy.newInstance();
        }
    }
}

public final class RingBufferWithValueStrategy<T> extends RingBufferFieldsByValue<T> implements RingBuffer<T> {
    public static final long INITIAL_CURSOR_VALUE = 0L;

    protected long p1, p2, p3, p4, p5, p6, p7;

    private final PaddedLong readSequencer = new PaddedLong(RingBufferWithValueStrategy.INITIAL_CURSOR_VALUE);

    private final PaddedLong writeSequencer = new PaddedLong(RingBufferWithValueStrategy.INITIAL_CURSOR_VALUE);

    private final PaddedLong availableSequencer = new PaddedLong(RingBufferWithValueStrategy.INITIAL_CURSOR_VALUE);

    private final ILogger logger;

    public RingBufferWithValueStrategy(
            int bufferSize,
            ILogger logger,
            DataTransferringStrategy<T> dataTransferringStrategy
    ) {
        super(bufferSize, dataTransferringStrategy);
        this.logger = logger;
    }

    @Override
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

    @Override
    public void commit(ProducerInputStream<T> chunk, int consumed) {
        long writerSequencerValue = this.writeSequencer.getValue();
        long availableSequencerValue = this.availableSequencer.getValue();

        int entriesStart = (int) (BUFFER_PAD + ((availableSequencerValue & indexMask)));
        int count = (int) (writerSequencerValue - availableSequencerValue);
        int window = entries.length - BUFFER_PAD - entriesStart;

        T[] buffer = ((BufferAware<T>) chunk).getBuffer();

        if (count <= window) {
            for (int i = 0; i < count; i++) {
                this.dataTransferringStrategy.copy(buffer[consumed + i], this.entries[entriesStart + i]);
            }
        } else {
            for (int i = 0; i < window; i++) {
                this.dataTransferringStrategy.copy(buffer[consumed + i], this.entries[entriesStart + i]);
            }

            for (int i = 0; i < count - window; i++) {
                this.dataTransferringStrategy.copy(buffer[consumed + window + i], this.entries[BUFFER_PAD + i]);
            }
        }

        this.availableSequencer.setValue(writerSequencerValue);
    }

    @Override
    public int fetch(T[] chunk) {
        long availableSequence = this.availableSequencer.getValue();
        long readerSequencerValue = this.readSequencer.getValue();

        int count = Math.min(chunk.length, (int) (availableSequence - readerSequencerValue));
        int entriesStart = (int) (BUFFER_PAD + ((readerSequencerValue & this.indexMask)));
        int window = this.entries.length - BUFFER_PAD - entriesStart;

        if (count <= window) {
            for (int i = 0; i < count; i++) {
                this.dataTransferringStrategy.copy(this.entries[entriesStart + i], chunk[i]);
                this.dataTransferringStrategy.clean(this.entries[entriesStart + i]);
            }
        } else {
            for (int i = 0; i < window; i++) {
                this.dataTransferringStrategy.copy(this.entries[entriesStart + i], chunk[i]);
                this.dataTransferringStrategy.clean(this.entries[entriesStart + i]);
            }

            for (int i = 0; i < count - window; i++) {
                this.dataTransferringStrategy.copy(this.entries[BUFFER_PAD + i], chunk[window + i]);
                this.dataTransferringStrategy.clean(this.entries[BUFFER_PAD + i]);
            }
        }

        this.readSequencer.setValue(this.readSequencer.getValue() + count);
        return count;
    }
}

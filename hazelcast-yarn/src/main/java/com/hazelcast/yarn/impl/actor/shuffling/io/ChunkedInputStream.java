package com.hazelcast.yarn.impl.actor.shuffling.io;

import java.util.Queue;
import java.util.Arrays;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.locks.LockSupport;

import com.hazelcast.internal.serialization.impl.HeapData;

public class ChunkedInputStream extends InputStream {
    private static final int BUFFER_OFFSET = HeapData.DATA_OFFSET;

    private byte[] buffer;
    private int bufferIdx = 0;

    private volatile boolean interrupted = false;
    private final Queue<byte[]> queue;

    public ChunkedInputStream(Queue<byte[]> queue) {
        this.queue = queue;
    }

    @Override
    public int read() throws IOException {
        if (this.interrupted) {
            return -1;
        }

        while (this.buffer == null) {
            if (this.interrupted) {
                return -1;
            }

            this.buffer = this.queue.poll();
            LockSupport.parkNanos(100);
        }

        if (this.bufferIdx == this.buffer.length - BUFFER_OFFSET - 1) {
            try {
                return this.buffer[BUFFER_OFFSET + this.bufferIdx] & 0xff;
            } finally {
                this.buffer = null;
                this.bufferIdx = 0;
            }
        } else {
            try {
                return this.buffer[BUFFER_OFFSET + this.bufferIdx] & 0xff;
            } finally {
                this.bufferIdx++;
            }
        }
    }

    public int remainingBytes() {
        return this.bufferIdx;
    }

    public void onOpen() {
        this.interrupted = false;
        if (this.buffer != null) {
            Arrays.fill(this.buffer, (byte) 0);
        }
        this.bufferIdx = 0;
    }

    public void markInterrupted() {
        this.interrupted = true;
    }
}

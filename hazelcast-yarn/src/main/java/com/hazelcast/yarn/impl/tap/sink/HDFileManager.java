package com.hazelcast.yarn.impl.tap.sink;


import java.io.File;
import java.io.RandomAccessFile;

import com.hazelcast.core.IFunction;
import com.hazelcast.util.IConcurrentMap;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.util.ConcurrentReferenceHashMap;
import com.hazelcast.yarn.api.tap.SinkTapWriteStrategy;

public class HDFileManager {
    private static final IConcurrentMap<String, HDFileManager> wrappers = new ConcurrentReferenceHashMap<String, HDFileManager>(
            ConcurrentReferenceHashMap.ReferenceType.STRONG,
            ConcurrentReferenceHashMap.ReferenceType.STRONG
    );

    private static final IFunction<String, HDFileManager> creator = new IFunction<String, HDFileManager>() {
        @Override
        public HDFileManager apply(String name) {
            return new HDFileManager(name);
        }
    };

    private final String name;

    public RandomAccessFile getFileWriter() {
        return fileWriter;
    }

    private RandomAccessFile fileWriter;

    private final AtomicInteger writersCount = new AtomicInteger(0);

    private final AtomicBoolean switcher = new AtomicBoolean(false);

    private volatile AtomicInteger pendingWriters = new AtomicInteger(0);

    public static HDFileManager getWrapper(String name) {
        return wrappers.applyIfAbsent(name, creator);
    }

    private HDFileManager(String name) {
        this.name = name;
    }

    public void open(SinkTapWriteStrategy sinkTapWriteStrategy) {
        if (this.switcher.compareAndSet(false, true)) {
            this.pendingWriters.set(this.writersCount.get());

            if (sinkTapWriteStrategy == SinkTapWriteStrategy.CLEAR_AND_REPLACE) {
                File file = new File(this.name);

                if (file.exists()) {
                    if (!file.delete()) {
                        throw new IllegalStateException("Can't delete file " + this.name);
                    }
                }
            }

            try {
                this.fileWriter = new RandomAccessFile(this.name, "rw");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void close() {
        if (this.pendingWriters.decrementAndGet() <= 0) {
            try {
                if (this.fileWriter != null) {
                    try {
                        this.fileWriter.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            } finally {
                this.fileWriter = null;
                this.switcher.set(false);
            }
        }
    }

    public void registerWriter() {
        this.writersCount.incrementAndGet();
    }
}

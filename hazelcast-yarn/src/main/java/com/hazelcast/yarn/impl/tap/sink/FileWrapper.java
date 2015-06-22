package com.hazelcast.yarn.impl.tap.sink;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.hazelcast.core.IFunction;
import com.hazelcast.util.IConcurrentMap;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.util.ConcurrentReferenceHashMap;
import com.hazelcast.yarn.api.tap.SinkTapWriteStrategy;

public class FileWrapper {
    private static final IConcurrentMap<String, FileWrapper> wrappers = new ConcurrentReferenceHashMap<String, FileWrapper>(
            ConcurrentReferenceHashMap.ReferenceType.STRONG,
            ConcurrentReferenceHashMap.ReferenceType.STRONG
    );

    private static final IFunction<String, FileWrapper> creator = new IFunction<String, FileWrapper>() {
        @Override
        public FileWrapper apply(String name) {
            return new FileWrapper(name);
        }
    };

    private final String name;
    private volatile FileWriter fileWriter;

    private final AtomicInteger writersCount = new AtomicInteger(0);
    private final AtomicBoolean switcher = new AtomicBoolean(false);

    private volatile AtomicInteger pendingWriters = new AtomicInteger(0);

    public static FileWrapper getWrapper(String name) {
        return wrappers.applyIfAbsent(name, creator);
    }

    private FileWrapper(String name) {
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
                this.fileWriter = new FileWriter(new File(this.name), true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void close() {
        if (this.pendingWriters.decrementAndGet() <= 0) {
            try {
                if (this.fileWriter != null) {
                    try {
                        this.fileWriter.flush();
                        this.fileWriter.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            } finally {
                this.fileWriter = null;
                this.switcher.set(false);
            }
        }
    }

    public FileWriter getFileWriter() {
        return this.fileWriter;
    }

    public void registerWriter() {
        this.writersCount.incrementAndGet();
    }
}

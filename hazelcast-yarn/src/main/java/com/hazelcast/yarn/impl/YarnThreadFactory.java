package com.hazelcast.yarn.impl;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class YarnThreadFactory implements ThreadFactory {
    private final String name;
    private final String hzName;
    private static final AtomicInteger threadCounter = new AtomicInteger(1);

    public YarnThreadFactory(String name, String hzName) {
        this.name = name;
        this.hzName = hzName;
    }

    @Override
    public Thread newThread(Runnable r) {
        SecurityManager s = System.getSecurityManager();
        ThreadGroup group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();

        Thread workerThread = new Thread(
                group,
                r,
                this.hzName + "-" + this.name + "-" + threadCounter.incrementAndGet()
        );

        workerThread.setDaemon(true);
        return workerThread;
    }
}

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

package com.hazelcast.jet.impl.executor;


import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.TimeUnit;

import com.hazelcast.logging.ILogger;

import java.util.concurrent.ThreadFactory;

import com.hazelcast.jet.api.executor.Task;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.jet.impl.JetThreadFactory;

import java.util.concurrent.atomic.AtomicInteger;

import java.util.concurrent.atomic.AtomicReference;

import com.hazelcast.jet.api.executor.WorkingProcessor;
import com.hazelcast.jet.api.executor.ApplicationExecutor;

import static com.hazelcast.util.Preconditions.checkTrue;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class DefaultApplicationExecutor implements ApplicationExecutor {
    protected final ILogger logger;

    protected final WorkingProcessor[] processors;

    protected volatile boolean executed;

    protected volatile boolean started;


    private final String name;

    private final NodeEngine nodeEngine;

    private final Thread[] workers;

    private final ThreadFactory threadFactory;

    private final AtomicInteger taskAmount = new AtomicInteger(0);

    private volatile int lastAddedIdx = -1;

    private final int awaitingTimeOut;

    private final AtomicReference<WorkingProcessor> unBalancedProcessor = new AtomicReference<WorkingProcessor>(null);


    public DefaultApplicationExecutor(String name,
                                      int threadNum,
                                      int awaitingTimeOut,
                                      NodeEngine nodeEngine) {
        checkNotNull(name);
        checkTrue(threadNum > 0, "Max thread count must be greater than zero");

        this.name = name;
        this.nodeEngine = nodeEngine;
        this.workers = new Thread[threadNum];
        String hzName = ((NodeEngineImpl) this.nodeEngine).getNode().hazelcastInstance.getName();
        this.threadFactory = new JetThreadFactory(name + "-executor", hzName);
        this.logger = nodeEngine.getLogger(DefaultApplicationExecutor.class);

        this.processors = new ApplicationTaskProcessor[this.workers.length];
        this.awaitingTimeOut = awaitingTimeOut;

        for (int i = 0; i < this.workers.length; i++) {
            this.processors[i] = createWorkingProcessor(threadNum);
            this.workers[i] = worker(processors[i]);
        }
    }

    protected WorkingProcessor createWorkingProcessor(int threadNum) {
        return new ApplicationTaskProcessor(threadNum, this.logger, this);
    }

    private Thread worker(Runnable processor) {
        return this.threadFactory.newThread(processor);
    }

    @Override
    public String getName() {
        return name;
    }

    protected void checkExecuted() {
        if (this.executed) {
            throw new IllegalStateException("Can't add new task after execution");
        }
    }

    @Override
    public void addDistributed(Task task) {
        checkExecuted();
        this.lastAddedIdx = (this.lastAddedIdx + 1) % (this.processors.length);
        this.processors[this.lastAddedIdx].consumeTask(task);
        this.taskAmount.incrementAndGet();
    }

    @Override
    public void addDistributed(Task[] tasks) {
        checkExecuted();
        for (Task task : tasks) {
            addDistributed(task);
        }
    }

    @Override
    public void execute() {
        this.started = true;
        this.executed = true;
        setBalanced();

        for (WorkingProcessor processor : this.processors) {
            processor.start();
        }
    }

    @Override
    public void shutdown() throws Exception {
        if (this.started) {
            for (WorkingProcessor processor : this.processors) {
                processor.shutdown().get(this.awaitingTimeOut, TimeUnit.SECONDS);
            }

            setBalanced();
        }
    }

    @Override
    public void interrupt() throws Exception {
        for (WorkingProcessor processor : this.processors) {
            processor.interrupt().get(this.awaitingTimeOut, TimeUnit.SECONDS);
        }

        setBalanced();
    }

    @Override
    public void markInterrupted() {
        for (WorkingProcessor processor : this.processors) {
            processor.markInterrupted();
        }
    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void startWorkers() {
        for (Thread worker : this.workers) {
            worker.start();
        }
    }

    @Override
    public boolean registerUnBalanced(WorkingProcessor unBalancedProcessor) {
        boolean result = this.unBalancedProcessor.compareAndSet(null, unBalancedProcessor);

        if (!result) {
            return false;
        }

        WorkingProcessor maxLoadedProcessor = null;

        for (WorkingProcessor processor : this.processors) {
            if (processor == unBalancedProcessor) {
                continue;
            }

            if (maxLoadedProcessor == null) {
                maxLoadedProcessor = processor;
            } else if (maxLoadedProcessor.getWorkingTaskCount() < processor.getWorkingTaskCount()) {
                maxLoadedProcessor = processor;
            }
        }

        if ((maxLoadedProcessor != null)
                && (maxLoadedProcessor.getWorkingTaskCount() - unBalancedProcessor.getWorkingTaskCount() > 2)) {
            return maxLoadedProcessor.balanceWith(unBalancedProcessor);
        } else {
            this.unBalancedProcessor.set(null);
            return true;
        }
    }

    protected NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public boolean isBalanced() {
        return this.unBalancedProcessor.get() == null;
    }

    @Override
    public void setThreadContextClassLoaders(ClassLoader classLoader) {
        for (Thread worker : this.workers) {
            worker.setContextClassLoader(classLoader);
        }
    }

    @Override
    public void setBalanced() {
        this.unBalancedProcessor.set(null);
    }

    @Override
    public String toString() {
        return this.name;
    }
}

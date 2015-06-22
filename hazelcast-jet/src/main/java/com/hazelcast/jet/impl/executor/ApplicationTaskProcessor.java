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

import java.util.List;
import java.util.Stack;
import java.util.ArrayList;
import java.util.concurrent.Future;

import com.hazelcast.logging.ILogger;

import java.util.concurrent.BlockingQueue;

import com.hazelcast.jet.api.executor.Task;
import com.hazelcast.jet.impl.SettableFuture;
import com.hazelcast.jet.api.executor.Payload;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.hazelcast.jet.api.actor.SleepingStrategy;
import com.hazelcast.jet.api.executor.WorkingProcessor;
import com.hazelcast.jet.api.executor.ApplicationExecutor;
import com.hazelcast.jet.impl.actor.strategy.AdaptiveSleepingStrategy;


import static com.hazelcast.util.Preconditions.checkTrue;

public class ApplicationTaskProcessor implements WorkingProcessor {
    protected volatile boolean interrupted = true;

    protected final BlockingQueue<Boolean> lockingQueue = new ArrayBlockingQueue<Boolean>(1);

    protected volatile boolean shutdown;

    protected final SettableFuture<Boolean> shutdownFuture = SettableFuture.create();

    protected volatile AtomicReference<SettableFuture<Boolean>> interruptedFuture =
            new AtomicReference<SettableFuture<Boolean>>(null);

    protected volatile Thread workingThread;

    protected final ILogger logger;

    protected final SleepingStrategy sleepingStrategy;

    protected final List<Task> tasks = new ArrayList<Task>();

    protected final AtomicInteger workingTaskCount = new AtomicInteger(0);

    protected final List<Task> originTasks = new ArrayList<Task>();

    protected volatile boolean hasIncoming;

    protected final Payload payload = new Payload() {
        private boolean produced;

        @Override
        public void set(boolean produced) {
            this.produced = produced;
        }

        @Override
        public boolean produced() {
            return produced;
        }
    };

    private volatile boolean balanced = true;

    private final AtomicReference<WorkingProcessor> unLoadedBalancer = new AtomicReference<WorkingProcessor>(null);

    private final Stack<Task> incomingTask = new Stack<Task>();

    private final ApplicationExecutor applicationExecutor;

    public ApplicationTaskProcessor(int threadNum,
                                    ILogger logger,
                                    ApplicationExecutor applicationExecutor) {
        checkTrue(threadNum >= 0, "threadNum must be positive");

        this.logger = logger;
        this.applicationExecutor = applicationExecutor;
        this.sleepingStrategy = new AdaptiveSleepingStrategy();
    }

    public Future<Boolean> shutdown() {
        this.shutdown = true;

        wakeUp();

        if ((this.workingThread != null) && (!interrupted)) {
            this.workingThread.interrupt();
        } else {
            this.interrupted = true;
        }

        return this.shutdownFuture;
    }

    public Future<Boolean> interrupt() {
        if (!interrupted) {
            if (this.workingThread != null) {
                this.workingThread.interrupt();
            }

            SettableFuture<Boolean> future = SettableFuture.create();

            if (this.interruptedFuture.compareAndSet(null, future)) {
                this.interrupted = true;
                return this.interruptedFuture.get();
            } else {
                future.set(true);
                return future;
            }
        } else {
            SettableFuture<Boolean> future = SettableFuture.create();
            future.set(true);
            return future;
        }
    }

    @Override
    public void markInterrupted() {
        this.interrupted = true;
    }

    public void await() throws InterruptedException {
        if (this.interruptedFuture.get() != null) {
            this.interruptedFuture.get().set(true);
            this.interruptedFuture.set(null);
        }

        this.lockingQueue.take();
    }

    public void start() {
        if (this.interrupted) {
            this.tasks.clear();
            this.balanced = true;
            this.interrupted = false;
            this.incomingTask.clear();
            this.tasks.addAll(this.originTasks);
            this.workingTaskCount.set(this.tasks.size());
            this.lockingQueue.offer(true);
        } else {
            this.lockingQueue.offer(true);
            throw new IllegalStateException("Can't start already started processor");
        }
    }


    @Override
    public void setBalanced(boolean balanced) {
        this.balanced = balanced;
    }

    @Override
    public void setHasIncoming(boolean hasIncoming) {
        this.hasIncoming = hasIncoming;
    }

    @Override
    public void wakeUp() {
        this.lockingQueue.offer(true);
    }

    @Override
    public boolean balanceWith(WorkingProcessor unBalancedProcessor) {
        return this.unLoadedBalancer.compareAndSet(null, unBalancedProcessor);
    }

    @Override
    public void acceptIncomingTask(Task task) {
        this.incomingTask.add(task);
    }

    protected boolean checkIncoming() {
        if (this.hasIncoming) {
            while (this.incomingTask.size() > 0) {
                this.tasks.add(this.incomingTask.pop());
                this.workingTaskCount.incrementAndGet();
            }

            this.hasIncoming = false;
            this.applicationExecutor.setBalanced();
            return true;
        } else {
            return false;
        }
    }

    private void balance() {
        if (!this.applicationExecutor.isBalanced()) {
            WorkingProcessor unLoadedBalancer = this.unLoadedBalancer.getAndSet(null);

            if (unLoadedBalancer != null) {
                if (executeBalancing(unLoadedBalancer)) {
                    return;
                }
            }

            checkIncoming();
        } else if (!this.balanced) {
            this.balanced = this.applicationExecutor.registerUnBalanced(this);
        }
    }

    private boolean executeBalancing(WorkingProcessor unLoadedBalancer) {
        int tasksSize = this.tasks.size();
        int delta = tasksSize - unLoadedBalancer.getWorkingTaskCount();

        if (delta >= 2) {
            for (int i = 0; i < delta / 2; i++) {
                if (this.tasks.size() > 0) {
                    unLoadedBalancer.acceptIncomingTask(this.tasks.remove(this.tasks.size() - 1));
                    this.workingTaskCount.decrementAndGet();
                }
            }

            /*
                Despite just a flag we also provide memory barrier -
                all memory which were changed by current thread (by all outcome tasks)
                Will be visible in accepted thread because of reading of this.hasIncoming variable
            */
            unLoadedBalancer.setHasIncoming(true);
        } else {
            unLoadedBalancer.setBalanced(false);

            if (!checkIncoming()) {
                this.applicationExecutor.setBalanced();
            }

            return true;
        }

        return false;
    }

    protected boolean execute() {
        balance();

        boolean payLoad = false;
        int i = 0;

        while (i < this.tasks.size()) {
            Task task = this.tasks.get(i);

            boolean activeTask = task.executeTask(this.payload);
            payLoad = payLoad || this.payload.produced();

            if (!activeTask) {
                this.balanced = false;
                this.workingTaskCount.decrementAndGet();
                this.tasks.remove(i);
            } else {
                i++;
            }
        }

        return payLoad;
    }

    public void run() {
        this.workingThread = Thread.currentThread();
        boolean wasPayLoad = false;

        try {
            while (!this.shutdown) {
                boolean payLoad = false;

                try {
                    if (this.interrupted) {
                        try {
                            await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }

                    payLoad = execute();

                    if (!payLoad) {
                        if (this.shutdown) {
                            break;
                        }

                        this.sleepingStrategy.await(wasPayLoad);
                    }
                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    this.logger.warning(e.getMessage(), e);
                }

                wasPayLoad = payLoad;
            }

            finalizeTasks();
        } finally {
            System.out.println("this.originTasks=" + this.originTasks);
            this.originTasks.clear();
            this.tasks.clear();
            this.shutdownFuture.set(true);
        }
    }

    private void finalizeTasks() {
        for (Task task : this.tasks) {
            task.finalizeTask();
        }
    }

    @Override
    public void consumeTask(Task entry) {
        this.originTasks.add(entry);
    }

    @Override
    public int getWorkingTaskCount() {
        return this.workingTaskCount.get();
    }
}

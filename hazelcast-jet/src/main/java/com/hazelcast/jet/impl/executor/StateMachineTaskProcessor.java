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

import com.hazelcast.logging.ILogger;

import java.util.concurrent.BlockingQueue;

import com.hazelcast.jet.api.executor.Task;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import com.hazelcast.jet.api.executor.ApplicationExecutor;

public class StateMachineTaskProcessor extends ApplicationTaskProcessor {
    protected final List<Task> incomingTask = new CopyOnWriteArrayList<Task>();
    private final BlockingQueue<Boolean> sleepingQueue = new ArrayBlockingQueue<Boolean>(1);

    public StateMachineTaskProcessor(int threadNum, ILogger logger, ApplicationExecutor applicationExecutor) {
        super(threadNum, logger, applicationExecutor);
        this.interrupted = false;
    }

    protected boolean checkIncoming() {
        while (this.incomingTask.size() > 0) {
            this.tasks.add(this.incomingTask.remove(0));
            this.workingTaskCount.incrementAndGet();
        }

        return true;
    }

    @Override
    public void consumeTask(Task task) {
        this.originTasks.add(task);
        this.incomingTask.add(task);
        wakeUp();
    }

    @Override
    public void start() {
        this.tasks.clear();
        this.tasks.addAll(this.originTasks);
        this.workingTaskCount.set(this.tasks.size());
        this.incomingTask.clear();
        this.interrupted = false;
        this.lockingQueue.offer(true);
    }

    protected boolean execute() {
        checkIncoming();

        boolean payLoad = false;
        int i = 0;

        while (i < this.tasks.size()) {
            Task task = this.tasks.get(i);

            boolean activeTask = task.executeTask(this.payload);
            payLoad = payLoad || this.payload.produced();

            if (!activeTask) {
                this.workingTaskCount.decrementAndGet();
                this.tasks.remove(i);
            } else {
                i++;
            }
        }

        return payLoad;
    }

    @Override
    public void wakeUp() {
        this.sleepingQueue.offer(true);
    }

    protected void sleep() throws InterruptedException {
        this.sleepingQueue.take();
    }

    public void run() {
        this.workingThread = Thread.currentThread();

        try {
            while (!this.shutdown) {
                try {
                    if (this.interrupted) {
                        await();
                    }

                    if (this.shutdown) {
                        break;
                    }

                    if (!execute()) {
                        sleep();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Throwable e) {
                    this.logger.warning(e.getMessage(), e);
                }
            }
        } finally {
            this.shutdownFuture.set(true);
        }
    }
}

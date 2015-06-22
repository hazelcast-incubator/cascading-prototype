package com.hazelcast.yarn.impl.executor;

import java.util.List;

import com.hazelcast.logging.ILogger;

import java.util.concurrent.BlockingQueue;

import com.hazelcast.yarn.api.executor.Task;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import com.hazelcast.yarn.api.executor.ApplicationExecutor;

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
        this.wakeUp();
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
        this.checkIncoming();

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

                    if (!this.execute()) {
                        this.sleep();
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

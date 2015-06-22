package com.hazelcast.yarn.api.executor;

import java.util.concurrent.Future;

public interface WorkingProcessor extends Runnable {
    void await() throws InterruptedException;

    void start();

    Future<Boolean> shutdown();

    Future<Boolean> interrupt();

    void markInterrupted();

    void consumeTask(Task task);

    int getWorkingTaskCount();

    void setBalanced(boolean balanced);

    boolean balanceWith(WorkingProcessor unBalancedProcessor);

    void acceptIncomingTask(Task task);

    void setHasIncoming(boolean hasIncoming);

    void wakeUp();
}

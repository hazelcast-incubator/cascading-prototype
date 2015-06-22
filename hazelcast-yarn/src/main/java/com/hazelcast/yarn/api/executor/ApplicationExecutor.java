package com.hazelcast.yarn.api.executor;


public interface ApplicationExecutor {
    String getName();

    void addDistributed(Task task);

    void addDistributed(Task task[]);

    void execute();

    void shutdown() throws Exception;

    void interrupt() throws Exception;

    void markInterrupted();

    void wakeUp();

    void startWorkers();

    boolean registerUnBalanced(WorkingProcessor applicationTaskProcessor);

    void setBalanced();

    boolean isBalanced();
}

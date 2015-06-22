package com.hazelcast.yarn.impl.executor;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.executor.WorkingProcessor;

public class StateMachineExecutor extends DefaultApplicationExecutor {
    public StateMachineExecutor(String name, int threadNum, NodeEngine nodeEngine) {
        super(name, threadNum, nodeEngine);

        this.startWorkers();
        this.execute();
    }

    @Override
    protected WorkingProcessor createWorkingProcessor(int threadNum) {
        return new StateMachineTaskProcessor(
                threadNum,
                this.logger,
                this
        );
    }

    @Override
    public void execute() {
        this.executed = true;

        for (WorkingProcessor processor : this.processors) {
            processor.start();
        }
    }

    @Override
    public void wakeUp() {
        for (WorkingProcessor processor : this.processors) {
            processor.wakeUp();
        }
    }

    @Override
    public void setBalanced() {

    }

    @Override
    protected void checkExecuted() {

    }
}

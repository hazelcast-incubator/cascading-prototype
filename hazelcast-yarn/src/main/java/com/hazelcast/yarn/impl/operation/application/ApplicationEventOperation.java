package com.hazelcast.yarn.impl.operation.application;

import java.io.IOException;

import com.hazelcast.spi.Operation;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.statemachine.ApplicationStateMachine;
import com.hazelcast.yarn.api.statemachine.application.ApplicationEvent;
import com.hazelcast.yarn.impl.statemachine.application.ApplicationStateMachineRequest;

public class ApplicationEventOperation extends AbstractYarnApplicationRequestOperation {
    private ApplicationEvent applicationEvent;

    public ApplicationEventOperation() {
    }

    public ApplicationEventOperation(ApplicationEvent applicationEvent, String name) {
        super(name);
        this.applicationEvent = applicationEvent;
    }

    @Override
    public void run() throws Exception {
        synchronized (Operation.class) {
            ApplicationContext context = resolveApplicationContext();
            ApplicationStateMachine applicationStateMachine = context.getApplicationStateMachine();
            long secondsToAwait = this.getNodeEngine().getConfig().getYarnApplicationConfig(getName()).getApplicationSecondsToAwait();
            Future future = applicationStateMachine.handleRequest(new ApplicationStateMachineRequest(applicationEvent));
            context.getApplicationStateMachineExecutor().wakeUp();
            future.get(secondsToAwait, TimeUnit.SECONDS);
        }
    }

    @Override
    public void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(this.applicationEvent);
    }

    @Override
    public void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        this.applicationEvent = in.readObject();
    }
}

package com.hazelcast.yarn.impl.operation.application;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;

import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.yarn.impl.statemachine.applicationmaster.requests.InterruptApplicationRequest;

public class InterruptExecutionOperation extends AbstractYarnApplicationRequestOperation {
    public InterruptExecutionOperation() {
    }

    public InterruptExecutionOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        ApplicationContext applicationContext = this.resolveApplicationContext();

        ApplicationMaster applicationMaster = applicationContext.getApplicationMaster();
        Future<ApplicationMasterResponse> future = applicationMaster.handleContainerRequest(new InterruptApplicationRequest());

        long secondsToAwait = this.getNodeEngine().getConfig().getYarnApplicationConfig(getName()).getApplicationSecondsToAwait();

        try {
            ApplicationMasterResponse response = future.get(secondsToAwait, TimeUnit.SECONDS);

            if (response != ApplicationMasterResponse.SUCCESS) {
                throw new IllegalStateException("Unable interrupt application's execution");
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        BlockingQueue<Object> mailBox = applicationMaster.getInterruptionMailBox();

        if (mailBox != null) {
            Object result = mailBox.poll(secondsToAwait, TimeUnit.SECONDS);
            if ((result != null) && (result instanceof Throwable)) {
                throw new RuntimeException((Throwable) result);
            }
        } else {
            throw new IllegalStateException("Unable interrupt application's execution (INVALIDATED)");
        }
    }
}

package com.hazelcast.yarn.impl.operation.application;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;

import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.yarn.impl.statemachine.applicationmaster.requests.ExecuteApplicationRequest;

public class ExecutionApplicationRequestOperation extends AbstractYarnApplicationRequestOperation {
    public ExecutionApplicationRequestOperation() {
    }

    public ExecutionApplicationRequestOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        ApplicationContext applicationContext = resolveApplicationContext();
        ApplicationMaster applicationMaster = applicationContext.getApplicationMaster();
        Future<ApplicationMasterResponse> future = applicationMaster.handleContainerRequest(new ExecuteApplicationRequest());

        long secondsToAwait = this.getNodeEngine().getConfig().getYarnApplicationConfig(getName()).getApplicationSecondsToAwait();

        //Waiting for until all containers started
        ApplicationMasterResponse response = future.get(secondsToAwait, TimeUnit.SECONDS);

        if (response != ApplicationMasterResponse.SUCCESS) {
            throw new IllegalStateException("Unable to start containers");
        }

        //Waiting for execution completion
        BlockingQueue<Object> mailBox = applicationMaster.getExecutionMailBox();

        if (mailBox != null) {
            Object result = mailBox.poll(secondsToAwait, TimeUnit.SECONDS);

            if ((result != null) && (result instanceof Throwable)) {
                throw new RuntimeException((Throwable) result);
            }

            if (response != ApplicationMasterResponse.SUCCESS) {
                throw new IllegalStateException("Unable to execute application");
            }
        } else {
            throw new IllegalStateException("Application has been invalidated");
        }
    }
}

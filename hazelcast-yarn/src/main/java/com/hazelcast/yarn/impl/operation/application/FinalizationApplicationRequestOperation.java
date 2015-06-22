package com.hazelcast.yarn.impl.operation.application;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.hazelcast.yarn.api.hazelcast.YarnService;
import com.hazelcast.yarn.api.YarnApplicationManager;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.yarn.impl.statemachine.applicationmaster.requests.FinalizeApplicationRequest;

public class FinalizationApplicationRequestOperation extends AbstractYarnApplicationRequestOperation {
    public FinalizationApplicationRequestOperation() {
        super();
    }

    public FinalizationApplicationRequestOperation(String name) {
        super(name);
    }

    private void destroyApplication() {
        YarnService yarnService = getService();
        YarnApplicationManager yarnApplicationManager = yarnService.getApplicationManager();
        yarnApplicationManager.destroyApplication(this.getName());
    }

    @Override
    public void run() throws Exception {
        ApplicationContext applicationContext = resolveApplicationContext();

        ApplicationMaster applicationMaster = applicationContext.getApplicationMaster();
        Future<ApplicationMasterResponse> future = applicationMaster.handleContainerRequest(new FinalizeApplicationRequest());

        long secondsToAwait = this.getNodeEngine().getConfig().getYarnApplicationConfig(getName()).getApplicationSecondsToAwait();

        try {
            ApplicationMasterResponse response = future.get(secondsToAwait, TimeUnit.SECONDS);

            if (response != ApplicationMasterResponse.SUCCESS) {
                throw new IllegalStateException("Unable to finalize application");
            }

        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            try {
                this.destroyApplication();
            } finally {
                applicationContext.getProcessingExecutor().shutdown();
                applicationContext.getTapStateMachineExecutor().shutdown();
                applicationContext.getApplicationStateMachineExecutor().shutdown();
                applicationContext.getTupleContainerStateMachineExecutor().shutdown();
                applicationContext.getApplicationMasterStateMachineExecutor().shutdown();
            }
        }
    }
}

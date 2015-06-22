package com.hazelcast.yarn.impl.statemachine.applicationmaster.processors;

import com.hazelcast.yarn.api.Dummy;
import com.hazelcast.yarn.api.container.ContainerPayLoadProcessor;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterResponse;

public class ExecutionCompletionProcessor implements ContainerPayLoadProcessor<Dummy> {
    private final ApplicationMaster applicationMaster;

    public ExecutionCompletionProcessor(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
    }

    @Override
    public void process(Dummy payload) throws Exception {
        this.applicationMaster.getApplicationContext().getProcessingExecutor().markInterrupted();
        this.applicationMaster.addToExecutionMailBox(ApplicationMasterResponse.SUCCESS);
    }
}

package com.hazelcast.yarn.impl.statemachine.applicationmaster.processors;

import com.hazelcast.yarn.api.container.ExecutionErrorHolder;
import com.hazelcast.yarn.api.container.ContainerPayLoadProcessor;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;

public class ExecutionErrorProcessor implements ContainerPayLoadProcessor<ExecutionErrorHolder> {
    private final ApplicationMaster applicationMaster;

    public ExecutionErrorProcessor(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
    }

    @Override
    public void process(ExecutionErrorHolder errorHolder) throws Exception {
        this.applicationMaster.setExecutionError(errorHolder);
    }
}

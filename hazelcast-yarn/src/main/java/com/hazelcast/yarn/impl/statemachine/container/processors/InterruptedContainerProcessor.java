package com.hazelcast.yarn.impl.statemachine.container.processors;

import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.container.ContainerPayLoadProcessor;
import com.hazelcast.yarn.api.container.applicationmaster.ApplicationMaster;

public class InterruptedContainerProcessor implements ContainerPayLoadProcessor<TupleContainer> {
    private final ApplicationMaster applicationMaster;

    public InterruptedContainerProcessor(TupleContainer container) {
        this.applicationMaster = container.getApplicationContext().getApplicationMaster();
    }

    @Override
    public void process(TupleContainer container) throws Exception {
        this.applicationMaster.handleContainerInterrupted(container);
    }
}

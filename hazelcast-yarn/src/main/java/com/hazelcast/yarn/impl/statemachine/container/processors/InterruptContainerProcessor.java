package com.hazelcast.yarn.impl.statemachine.container.processors;

import com.hazelcast.yarn.api.Dummy;
import com.hazelcast.yarn.api.container.ContainerTask;
import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.container.ContainerPayLoadProcessor;

public class InterruptContainerProcessor implements ContainerPayLoadProcessor<Dummy> {
    private final TupleContainer container;

    public InterruptContainerProcessor(TupleContainer container) {
        this.container = container;
    }

    @Override
    public void process(Dummy payload) throws Exception {
        for (ContainerTask containerTask : this.container.getContainerTasks()) {
            containerTask.interrupt();
        }
    }
}
package com.hazelcast.yarn.impl.statemachine.container.processors;

import com.hazelcast.yarn.api.Dummy;
import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.container.ContainerPayLoadProcessor;

public class ExecuteContainerProcessor implements ContainerPayLoadProcessor<Dummy> {
    private final TupleContainer container;

    public ExecuteContainerProcessor(TupleContainer container) {
        this.container = container;
    }

    @Override
    public void process(Dummy payload) throws Exception {
        this.container.execute();
    }
}

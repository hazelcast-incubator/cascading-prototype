package com.hazelcast.yarn.impl.statemachine.container.processors;

import com.hazelcast.yarn.api.Dummy;
import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.container.ContainerPayLoadProcessor;

public class StartTupleContainerProcessor implements ContainerPayLoadProcessor<Dummy> {
    private final TupleContainer container;

    public StartTupleContainerProcessor(TupleContainer container) {
        this.container = container;
    }

    @Override
    public void process(Dummy payLoad) throws Exception {
        this.container.start();
    }
}

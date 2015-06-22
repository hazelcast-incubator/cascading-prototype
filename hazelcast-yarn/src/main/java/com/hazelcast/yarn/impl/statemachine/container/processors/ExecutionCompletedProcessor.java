package com.hazelcast.yarn.impl.statemachine.container.processors;

import java.util.List;

import com.hazelcast.yarn.api.Dummy;
import com.hazelcast.yarn.api.container.TupleChannel;
import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.container.ContainerPayLoadProcessor;

public class ExecutionCompletedProcessor implements ContainerPayLoadProcessor<Dummy> {
    private final TupleContainer container;
    private final ApplicationContext applicationContext;

    public ExecutionCompletedProcessor(TupleContainer container) {
        this.container = container;
        this.applicationContext = container.getApplicationContext();
    }

    @Override
    public void process(Dummy payLoad) throws Exception {  //payload - completed container
        List<TupleChannel> channels = this.container.getOutputChannels();

        if (channels.size() > 0) {
            for (int idx = 0; idx < channels.size(); idx++) { // We don't use foreach to prevent iterator creation
                channels.get(idx).close();
            }
        }

        this.applicationContext.getApplicationMaster().handleContainerCompleted(this.container);
    }
}

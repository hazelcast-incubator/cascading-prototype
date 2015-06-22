package com.hazelcast.yarn.impl.container;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.TupleFactory;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.processor.TupleContainerProcessorFactory;

public class DefaultTupleContainer extends AbstractTupleContainer {
    public DefaultTupleContainer(Vertex vertex,
                                 TupleContainerProcessorFactory containerProcessorFactory,
                                 NodeEngine nodeEngine,
                                 ApplicationContext applicationContext, TupleFactory tupleFactory) {
        super(vertex, containerProcessorFactory, nodeEngine, applicationContext, tupleFactory);
    }

    protected void wakeUpExecutor() {
        getApplicationContext().getTupleContainerStateMachineExecutor().wakeUp();
    }
}
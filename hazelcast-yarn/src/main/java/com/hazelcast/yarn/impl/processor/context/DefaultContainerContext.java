package com.hazelcast.yarn.impl.processor.context;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.TupleFactory;
import com.hazelcast.yarn.api.container.ContainerContext;
import com.hazelcast.yarn.api.application.ApplicationContext;

public class DefaultContainerContext implements ContainerContext {
    private final int id;
    private final Vertex vertex;
    private final NodeEngine nodeEngine;
    private final TupleFactory tupleFactory;
    private final ApplicationContext applicationContext;

    public DefaultContainerContext(NodeEngine nodeEngine,
                                   ApplicationContext applicationContext,
                                   int id,
                                   Vertex vertex,
                                   TupleFactory tupleFactory) {
        this.id = id;
        this.vertex = vertex;
        this.nodeEngine = nodeEngine;
        this.tupleFactory = tupleFactory;
        this.applicationContext = applicationContext;
    }

    @Override
    public NodeEngine getNodeEngine() {
        return this.nodeEngine;
    }

    @Override
    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    @Override
    public int getID() {
        return id;
    }

    @Override
    public Vertex getVertex() {
        return vertex;
    }

    @Override
    public TupleFactory getTupleFactory() {
        return tupleFactory;
    }
}

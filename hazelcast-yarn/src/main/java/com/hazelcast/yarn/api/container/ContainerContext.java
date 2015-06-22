package com.hazelcast.yarn.api.container;

import com.hazelcast.spi.NodeEngine;


import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.TupleFactory;

public interface ContainerContext {
    NodeEngine getNodeEngine();

    ApplicationContext getApplicationContext();

    int getID();

    Vertex getVertex();

    TupleFactory getTupleFactory();
}

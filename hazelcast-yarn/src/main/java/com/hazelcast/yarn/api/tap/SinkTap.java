package com.hazelcast.yarn.api.tap;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.tuple.TupleWriter;
import com.hazelcast.yarn.api.container.ContainerContext;

public abstract class SinkTap implements Tap {
    public abstract TupleWriter[] getWriters(NodeEngine nodeEngine, ContainerContext applicationContext);

    public boolean isSource() {
        return false;
    }

    public boolean isSink() {
        return true;
    }

    public abstract SinkTapWriteStrategy getTapStrategy();
}

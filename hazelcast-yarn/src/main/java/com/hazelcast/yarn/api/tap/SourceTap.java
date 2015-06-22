package com.hazelcast.yarn.api.tap;

import com.hazelcast.yarn.api.dag.Vertex;
import com.hazelcast.yarn.api.tuple.TupleFactory;
import com.hazelcast.yarn.api.tuple.TupleReader;
import com.hazelcast.yarn.api.application.ApplicationContext;

public abstract class SourceTap implements Tap {
    public abstract TupleReader[] getReaders(ApplicationContext applicationContext, Vertex vertex, TupleFactory tupleFactory);

    public boolean isSource() {
        return true;
    }

    public boolean isSink() {
        return false;
    }
}

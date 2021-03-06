package com.hazelcast.yarn.impl.statemachine.container.requests;

import com.hazelcast.yarn.api.Dummy;
import com.hazelcast.yarn.api.statemachine.container.ContainerRequest;
import com.hazelcast.yarn.api.statemachine.container.tuplecontainer.TupleContainerEvent;

public class StartTupleContainerRequest implements ContainerRequest<TupleContainerEvent, Dummy> {
    @Override
    public TupleContainerEvent getContainerEvent() {
        return TupleContainerEvent.START;
    }

    @Override
    public Dummy getPayLoad() {
        return Dummy.INSTANCE;
    }
}

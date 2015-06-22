package com.hazelcast.yarn.impl.statemachine.container.requests;

import com.hazelcast.yarn.api.Dummy;
import com.hazelcast.yarn.api.statemachine.container.ContainerRequest;
import com.hazelcast.yarn.api.statemachine.container.tuplecontainer.TupleContainerEvent;

public class InvalidateContainersRequest
        implements ContainerRequest<TupleContainerEvent, Dummy> {
    @Override
    public TupleContainerEvent getContainerEvent() {
        return TupleContainerEvent.INVALIDATE_CONTAINER;
    }

    @Override
    public Dummy getPayLoad() {
        return Dummy.INSTANCE;
    }
}


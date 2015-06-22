package com.hazelcast.yarn.impl.statemachine.container.requests;

import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.statemachine.container.ContainerRequest;
import com.hazelcast.yarn.api.statemachine.container.tuplecontainer.TupleContainerEvent;

public class NotifyContainerFinalizedRequest implements ContainerRequest<TupleContainerEvent, TupleContainer> {
    private final TupleContainer tupleContainer;

    public NotifyContainerFinalizedRequest(TupleContainer tupleContainer) {
        this.tupleContainer = tupleContainer;
    }

    @Override
    public TupleContainerEvent getContainerEvent() {
        return TupleContainerEvent.FINALIZE;
    }

    @Override
    public TupleContainer getPayLoad() {
        return this.tupleContainer;
    }
}

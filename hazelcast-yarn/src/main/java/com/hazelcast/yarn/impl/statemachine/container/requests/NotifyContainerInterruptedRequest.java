package com.hazelcast.yarn.impl.statemachine.container.requests;


import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.statemachine.container.ContainerRequest;
import com.hazelcast.yarn.api.statemachine.container.tuplecontainer.TupleContainerEvent;

public class NotifyContainerInterruptedRequest implements ContainerRequest<TupleContainerEvent, TupleContainer> {
    private final TupleContainer tupleContainer;

    public NotifyContainerInterruptedRequest(TupleContainer tupleContainer) {
        this.tupleContainer = tupleContainer;
    }

    @Override
    public TupleContainerEvent getContainerEvent() {
        return TupleContainerEvent.INTERRUPTED;
    }

    @Override
    public TupleContainer getPayLoad() {
        return this.tupleContainer;
    }
}

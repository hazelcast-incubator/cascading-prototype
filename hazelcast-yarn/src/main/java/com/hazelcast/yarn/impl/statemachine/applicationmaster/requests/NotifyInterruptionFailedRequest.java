package com.hazelcast.yarn.impl.statemachine.applicationmaster.requests;

import com.hazelcast.yarn.api.container.TupleContainer;
import com.hazelcast.yarn.api.statemachine.container.ContainerRequest;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterEvent;

public class NotifyInterruptionFailedRequest implements ContainerRequest<ApplicationMasterEvent, TupleContainer> {
    private final TupleContainer container;

    public NotifyInterruptionFailedRequest(TupleContainer container) {
        this.container = container;
    }

    @Override
    public ApplicationMasterEvent getContainerEvent() {
        return ApplicationMasterEvent.INTERRUPTION_FAILURE;
    }

    @Override
    public TupleContainer getPayLoad() {
        return this.container;
    }
}

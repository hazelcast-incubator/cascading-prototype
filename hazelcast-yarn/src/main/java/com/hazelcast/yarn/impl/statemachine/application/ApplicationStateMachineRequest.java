package com.hazelcast.yarn.impl.statemachine.application;

import com.hazelcast.yarn.api.Dummy;
import com.hazelcast.yarn.api.statemachine.StateMachineRequest;
import com.hazelcast.yarn.api.statemachine.application.ApplicationEvent;

public class ApplicationStateMachineRequest implements StateMachineRequest<ApplicationEvent, Dummy> {
    private final ApplicationEvent event;

    public ApplicationStateMachineRequest(ApplicationEvent event) {
        this.event = event;
    }

    @Override
    public ApplicationEvent getContainerEvent() {
        return this.event;
    }

    @Override
    public Dummy getPayLoad() {
        return Dummy.INSTANCE;
    }
}

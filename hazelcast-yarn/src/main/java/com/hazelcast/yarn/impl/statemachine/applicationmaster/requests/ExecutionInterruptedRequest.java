package com.hazelcast.yarn.impl.statemachine.applicationmaster.requests;

import com.hazelcast.yarn.api.Dummy;
import com.hazelcast.yarn.api.statemachine.container.ContainerRequest;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterEvent;

public class ExecutionInterruptedRequest implements ContainerRequest<ApplicationMasterEvent, Dummy> {
    @Override
    public ApplicationMasterEvent getContainerEvent() {
        return ApplicationMasterEvent.EXECUTION_INTERRUPTED;
    }

    @Override
    public Dummy getPayLoad() {
        return Dummy.INSTANCE;
    }
}

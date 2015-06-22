package com.hazelcast.yarn.impl.statemachine.applicationmaster.requests;


import com.hazelcast.yarn.api.statemachine.container.ContainerRequest;
import com.hazelcast.yarn.api.Dummy;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterEvent;

public class FinalizeApplicationRequest implements ContainerRequest<ApplicationMasterEvent, Dummy> {
    @Override
    public ApplicationMasterEvent getContainerEvent() {
        return ApplicationMasterEvent.FINALIZE;
    }

    @Override
    public Dummy getPayLoad() {
        return Dummy.INSTANCE;
    }
}
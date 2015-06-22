package com.hazelcast.yarn.api.statemachine.container.applicationmaster;

import com.hazelcast.yarn.api.statemachine.container.ContainerStateMachineFactory;

public interface ApplicationMasterStateMachineFactory extends ContainerStateMachineFactory<ApplicationMasterEvent, ApplicationMasterState, ApplicationMasterResponse> {

}

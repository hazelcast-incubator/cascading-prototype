package com.hazelcast.yarn.api.statemachine;

import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterState;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterEvent;
import com.hazelcast.yarn.api.statemachine.container.applicationmaster.ApplicationMasterResponse;

public interface AppMasterStateMachine extends ContainerStateMachine<ApplicationMasterEvent, ApplicationMasterState, ApplicationMasterResponse> {
}

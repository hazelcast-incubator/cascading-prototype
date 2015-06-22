package com.hazelcast.yarn.api.statemachine;

import com.hazelcast.yarn.api.statemachine.application.ApplicationState;
import com.hazelcast.yarn.api.statemachine.application.ApplicationEvent;
import com.hazelcast.yarn.api.statemachine.application.ApplicationResponse;

public interface ApplicationStateMachine extends StateMachine<ApplicationEvent, ApplicationState, ApplicationResponse> {
}

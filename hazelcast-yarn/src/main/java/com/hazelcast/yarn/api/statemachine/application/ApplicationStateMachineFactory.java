package com.hazelcast.yarn.api.statemachine.application;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.statemachine.ApplicationStateMachine;

import com.hazelcast.yarn.api.statemachine.StateMachineFactory;
import com.hazelcast.yarn.api.statemachine.StateMachineRequestProcessor;

public interface ApplicationStateMachineFactory extends StateMachineFactory<ApplicationEvent, ApplicationState, ApplicationResponse> {
    ApplicationStateMachine newStateMachine(String name, StateMachineRequestProcessor<ApplicationEvent> processor, NodeEngine nodeEngine, ApplicationContext applicationContext);
}

package com.hazelcast.yarn.api.statemachine.container;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.container.ContainerResponse;
import com.hazelcast.yarn.api.statemachine.StateMachineFactory;
import com.hazelcast.yarn.api.statemachine.ContainerStateMachine;
import com.hazelcast.yarn.api.statemachine.StateMachineRequestProcessor;

public interface ContainerStateMachineFactory<Input extends ContainerEvent, State extends ContainerState, Output extends ContainerResponse> extends StateMachineFactory<Input, State, Output> {
    ContainerStateMachine<Input, State, Output> newStateMachine(String name, StateMachineRequestProcessor<Input> processor, NodeEngine nodeEngine, ApplicationContext applicationContext);
}

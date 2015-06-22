package com.hazelcast.yarn.api.statemachine.container.tuplecontainer;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.statemachine.TupleContainerStateMachine;
import com.hazelcast.yarn.api.statemachine.StateMachineRequestProcessor;
import com.hazelcast.yarn.api.statemachine.container.ContainerStateMachineFactory;

public interface TupleContainerStateMachineFactory extends ContainerStateMachineFactory<TupleContainerEvent, TupleContainerState, TupleContainerResponse> {
    TupleContainerStateMachine newStateMachine(String name, StateMachineRequestProcessor<TupleContainerEvent> processor, NodeEngine nodeEngine, ApplicationContext applicationContext);
}

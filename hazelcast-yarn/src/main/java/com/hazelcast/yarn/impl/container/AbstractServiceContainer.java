package com.hazelcast.yarn.impl.container;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.container.Container;
import com.hazelcast.yarn.api.container.ContainerResponse;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.statemachine.ContainerStateMachine;
import com.hazelcast.yarn.api.statemachine.StateMachineFactory;
import com.hazelcast.yarn.api.statemachine.container.ContainerState;
import com.hazelcast.yarn.api.statemachine.container.ContainerEvent;
import com.hazelcast.yarn.api.statemachine.container.ContainerStateMachineFactory;

public abstract class AbstractServiceContainer<SI extends ContainerEvent, SS extends ContainerState, SO extends ContainerResponse> extends AbstractContainer<SI, SS, SO> {
    public AbstractServiceContainer(ContainerStateMachineFactory<SI, SS, SO> stateMachineFactory, NodeEngine nodeEngine, ApplicationContext applicationContext) {
        super(stateMachineFactory, nodeEngine, applicationContext,null);
    }

    public void addPredecessor(Container container) {
        throw new UnsupportedOperationException("Service container can't have predecessors");
    }
}

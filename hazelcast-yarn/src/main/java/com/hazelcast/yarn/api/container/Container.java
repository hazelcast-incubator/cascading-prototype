package com.hazelcast.yarn.api.container;

import java.util.List;
import java.util.Set;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.yarn.api.statemachine.StateMachine;
import com.hazelcast.yarn.api.application.ApplicationContext;
import com.hazelcast.yarn.api.statemachine.container.ContainerEvent;
import com.hazelcast.yarn.api.statemachine.container.ContainerState;

public interface Container<SI extends ContainerEvent, SS extends ContainerState, SO extends ContainerResponse> extends ContainerRequestHandler<SI, SO>, ContainerStateMachineRequestProcessor<SI> {
    NodeEngine getNodeEngine();

    StateMachine<SI, SS, SO> getStateMachine();

    ApplicationContext getApplicationContext();

    List<TupleContainer> getFollowers();

    List<TupleContainer> getPredecessors();

    void addFollower(TupleContainer container);

    void addPredecessor(TupleContainer container);

    ContainerContext getContainerContext();

    int getID();
}

package com.hazelcast.yarn.api.statemachine;

public interface StateMachineRequest<E extends StateMachineEvent, P> {
    E getContainerEvent();

    P getPayLoad();
}

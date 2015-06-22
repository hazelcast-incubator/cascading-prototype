package com.hazelcast.yarn.api.statemachine.container;

import com.hazelcast.yarn.api.statemachine.StateMachineRequest;

public interface ContainerRequest<E extends ContainerEvent, P> extends StateMachineRequest<E, P> {
}

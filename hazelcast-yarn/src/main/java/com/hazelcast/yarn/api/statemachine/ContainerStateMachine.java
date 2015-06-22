package com.hazelcast.yarn.api.statemachine;

import com.hazelcast.yarn.api.statemachine.container.ContainerEvent;
import com.hazelcast.yarn.api.statemachine.container.ContainerState;
import com.hazelcast.yarn.api.container.ContainerResponse;

public interface ContainerStateMachine<I extends ContainerEvent, S extends ContainerState, O extends ContainerResponse> extends StateMachine<I, S, O> {
}

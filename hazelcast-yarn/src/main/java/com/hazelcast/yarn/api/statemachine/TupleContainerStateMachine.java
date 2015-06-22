package com.hazelcast.yarn.api.statemachine;

import com.hazelcast.yarn.api.statemachine.container.tuplecontainer.TupleContainerEvent;
import com.hazelcast.yarn.api.statemachine.container.tuplecontainer.TupleContainerResponse;
import com.hazelcast.yarn.api.statemachine.container.tuplecontainer.TupleContainerState;

public interface TupleContainerStateMachine extends ContainerStateMachine<TupleContainerEvent, TupleContainerState, TupleContainerResponse> {
}

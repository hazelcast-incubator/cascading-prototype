package com.hazelcast.yarn.api.statemachine.container.tuplecontainer;

import com.hazelcast.yarn.api.statemachine.container.ContainerState;

public enum TupleContainerState implements ContainerState {
    NEW,
    INTERRUPTING,
    AWAITING,
    EXECUTION,
    INVALIDATED,
    FINALIZED
}
